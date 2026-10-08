import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  OrchestrationV2Command,
  RunId,
  ScheduledTaskId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as Recovery from "../prism/RecoveryHistory.ts";
import {
  followRun,
  reportsHold,
  resolveSchedulerRun,
  ScheduledTaskSpectra,
  schedulerRunOpenGuard,
  type BoundSpectrum,
} from "./handoff.ts";
import type { CheckState } from "./state.ts";
import { ensureCheckSchema, writeCheckState } from "./store.ts";

const toJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const threadId = ThreadId.make("thread:handoff");
let ordinal = 0;

/**
 * A projection run row as the follower reads it; links live in its payload.
 * `startedAt` is the run's own timestamp, which upstream sets before delivery.
 */
const run = (input: {
  readonly id: string;
  readonly status: string;
  readonly startedAt?: boolean;
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
        startedAt: input.startedAt === true ? "2026-10-08T12:00:01.000Z" : null,
        ...(input.restartOf === undefined ? {} : { restartContinuationOfRunId: input.restartOf }),
        ...(input.prismSourceOf === undefined
          ? {}
          : { forkPrismContinuationSourceRunId: input.prismSourceOf }),
      }),
    })}`;
  });

/** A provider turn recorded for the run's attempt, as adapters do once the provider takes the prompt. */
const providerTurn = (runId: string, status: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    ordinal += 1;
    yield* sql`INSERT OR IGNORE INTO orchestration_v2_projection_run_attempts ${sql.insert({
      attempt_id: `attempt:${runId}`,
      thread_id: threadId,
      run_id: runId,
      attempt_ordinal: 1,
      root_node_id: `node:${runId}`,
      provider: "codex",
      provider_thread_id: "provider-thread",
      provider_turn_id: null,
      status: "running",
      payload_json: "{}",
    })}`;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_turns ${sql.insert({
      provider_turn_id: `turn:${runId}:${ordinal}`,
      thread_id: threadId,
      provider_thread_id: "provider-thread",
      node_id: `node:${runId}`,
      run_attempt_id: `attempt:${runId}`,
      ordinal,
      status,
      started_at: "2026-10-08T12:00:02.000Z",
      completed_at: null,
      payload_json: "{}",
    })}`;
  });

const setup = Effect.gen(function* () {
  yield* Recovery.initializeRecoveryHistory;
  yield* ensureCheckSchema;
});

it.effect("a failed run holds until its recovery decision is conclusive", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* run({ id: "run:failed", status: "failed", startedAt: true });
    yield* providerTurn("run:failed", "failed");
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
    assert.deepEqual(yield* followRun("run:failed"), {
      kind: "ended",
      runId: "run:failed",
      started: true,
    });
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("an automatic abandon ends the work for the scheduler to decide", () =>
  Effect.gen(function* () {
    yield* setup;
    // The run's own startedAt is set before delivery; without a provider turn it never started.
    yield* run({ id: "run:stopped", status: "failed", startedAt: true });
    yield* Recovery.writeRecoveryOutcome({
      sourceRunId: RunId.make("run:stopped"),
      threadId,
      status: "decided",
      outcome: "abandoned",
      reason: "stopped_or_retired",
    });
    assert.deepEqual(yield* followRun("run:stopped"), {
      kind: "ended",
      runId: "run:stopped",
      started: false,
    });
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a retry is followed only to the exact successor that names its source", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* run({ id: "run:source", status: "failed", startedAt: true });
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
      started: false,
    });
    // Delivery anywhere along the chain counts.
    yield* providerTurn("run:source", "failed");
    assert.deepEqual(yield* followRun("run:source"), {
      kind: "completed",
      runId: "run:successor",
      started: true,
    });
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("upstream's restart continuation is followed before any recovery fact", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* run({ id: "run:cancelled", status: "cancelled", startedAt: true });
    yield* run({ id: "run:restart", status: "running", restartOf: "run:cancelled" });
    assert.deepEqual(yield* followRun("run:cancelled"), { kind: "waiting" });
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE orchestration_v2_projection_runs SET status = 'completed'
      WHERE run_id = 'run:restart'`;
    yield* providerTurn("run:restart", "completed");
    assert.deepEqual(yield* followRun("run:cancelled"), {
      kind: "completed",
      runId: "run:restart",
      started: true,
    });
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

/**
 * #176's published Spectrum storage (toolboxmd/chromeria 7cb115d306,
 * `spectrum/store.ts` and `spectrum/state.ts`): its exact table, and payloads
 * written as its encoder writes them. #176 owns the real adapter; this fixture
 * maps the same rows onto the fence's port.
 */
const ensureSpectraFixture = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS fork_spectra (
    thread_id TEXT PRIMARY KEY,
    caller_thread_id TEXT NOT NULL,
    scheduler_run_id TEXT,
    generation INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    payload_json TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX IF NOT EXISTS fork_spectra_caller ON fork_spectra(caller_thread_id, scheduler_run_id)`;
});

const FixtureReport = OrchestrationV2Command.pipe(
  Schema.refine(
    (command): command is Extract<OrchestrationV2Command, { type: "message.dispatch" }> =>
      command.type === "message.dispatch",
  ),
);
/** The fields of #176's `SpectrumState` that the fence reads; missing report fields decode null. */
const decodeFixtureSpectrum = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      status: Schema.Literals(["active", "settled", "retired"]),
      outbox: Schema.Array(Schema.Struct({ commandId: CommandId })),
      report: Schema.NullOr(FixtureReport).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
      reportAbandonment: Schema.NullOr(
        Schema.Struct({
          commandId: CommandId,
          person: Schema.String,
          abandonedAt: Schema.DateTimeUtcFromString,
        }),
      ).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
    }),
  ),
);

const fixtureSpectra = {
  boundTo: (schedulerRunId: string) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const rows = yield* sql<{ readonly payload_json: string }>`
        SELECT payload_json FROM fork_spectra WHERE scheduler_run_id = ${schedulerRunId}
      `;
      return rows.map((row): BoundSpectrum => {
        const state = decodeFixtureSpectrum(row.payload_json);
        const report = state.report;
        return {
          status: state.status,
          report:
            report === null
              ? null
              : {
                  commandId: report.commandId,
                  messageId: report.messageId,
                  threadId: report.threadId,
                  inOutbox: state.outbox.some((command) => command.commandId === report.commandId),
                },
          reportAbandonment:
            state.reportAbandonment === null
              ? null
              : { commandId: state.reportAbandonment.commandId },
        };
      });
    }),
};

const encodeCommand = Schema.encodeSync(OrchestrationV2Command);
/** A report attempt as Spectrum dispatches it to the caller's thread. */
const reportCommand = (id: string) => ({
  type: "message.dispatch" as const,
  commandId: CommandId.make(`report:${id}`),
  threadId,
  messageId: MessageId.make(`report-message:${id}`),
  createdBy: "system" as const,
  creationSource: "server" as const,
  text: "Spectrum report",
  attachments: [],
  dispatchMode: { type: "queue_after_active" as const },
});

let spectra = 0;
/** One Spectrum row bound to a scheduler run, in #176's full payload shape. */
const bindSpectrum = (
  schedulerRunId: string,
  input: {
    readonly status: "active" | "settled" | "retired";
    readonly report?: string;
    readonly inOutbox?: boolean;
    readonly abandoned?: string;
    /** A row written before the report fields existed. */
    readonly legacy?: boolean;
  },
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    spectra += 1;
    const spectrumThreadId = `spectrum:${spectra}`;
    const report = input.report === undefined ? null : encodeCommand(reportCommand(input.report));
    const selection = { instanceId: "codex", model: "test-model" };
    const payload = {
      version: 1,
      threadId: spectrumThreadId,
      callerThreadId: threadId,
      callerRunId: "run:caller",
      scheduledTaskId: "task:bound",
      schedulerRunId,
      question: "What should we build?",
      mode: "council",
      limit: 2,
      moderator: 1,
      participants: [
        { threadId: `${spectrumThreadId}:blue`, label: "Blue", selection },
        { threadId: `${spectrumThreadId}:red`, label: "Red", selection },
      ],
      generation: 0,
      revision: 0,
      cursor: 0,
      status: input.status,
      cycle: 0,
      round: null,
      transcript: [],
      inbox: [],
      outbox: report !== null && input.inOutbox === true ? [report] : [],
      ...(input.legacy === true
        ? {}
        : {
            report,
            reportAbandonment:
              input.abandoned === undefined
                ? null
                : {
                    commandId: `report:${input.abandoned}`,
                    person: "user",
                    abandonedAt: "2026-10-08T12:30:00.000Z",
                  },
          }),
    };
    yield* sql`INSERT INTO fork_spectra ${sql.insert({
      thread_id: spectrumThreadId,
      caller_thread_id: threadId,
      scheduler_run_id: schedulerRunId,
      generation: 0,
      revision: 0,
      payload_json: toJson(payload),
    })}`;
  });

const holds = (schedulerRunId: string) =>
  reportsHold(schedulerRunId).pipe(Effect.provideService(ScheduledTaskSpectra, fixtureSpectra));

const receipt = (id: string, status: "accepted" | "rejected") =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_v2_command_receipts ${sql.insert({
      command_id: `report:${id}`,
      thread_id: threadId,
      command_type: "message.dispatch",
      accepted_at: "2026-10-08T12:00:00.000Z",
      result_sequence: 1,
      status,
      error: null,
    })}`;
  });

it.effect("the completion fence holds through Spectrum, dispatch and the report's turn", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* ensureSpectraFixture;
    assert.isFalse(yield* holds("scheduler:none"));
    yield* bindSpectrum("scheduler:active", { status: "active" });
    assert.isTrue(yield* holds("scheduler:active"));
    // A retired Spectrum that wrote no report does not hold, nor does a row from before reports.
    yield* bindSpectrum("scheduler:retired", { status: "retired" });
    yield* bindSpectrum("scheduler:retired", { status: "retired", legacy: true });
    assert.isFalse(yield* holds("scheduler:retired"));
    yield* bindSpectrum("scheduler:outbox", {
      status: "settled",
      report: "outbox",
      inOutbox: true,
    });
    assert.isTrue(yield* holds("scheduler:outbox"));
    // Drained from the outbox with no receipt yet, or rejected awaiting Spectrum's next attempt.
    yield* bindSpectrum("scheduler:pending", { status: "settled", report: "pending" });
    assert.isTrue(yield* holds("scheduler:pending"));
    yield* receipt("rejected", "rejected");
    yield* bindSpectrum("scheduler:rejected", { status: "settled", report: "rejected" });
    assert.isTrue(yield* holds("scheduler:rejected"));
    // Accepted: the report's own run decides, and only a provider turn proves delivery.
    yield* receipt("turn", "accepted");
    yield* bindSpectrum("scheduler:turn", { status: "settled", report: "turn" });
    assert.isTrue(yield* holds("scheduler:turn"));
    yield* run({
      id: "run:report",
      status: "running",
      startedAt: true,
      userMessageId: "report-message:turn",
    });
    yield* providerTurn("run:report", "running");
    assert.isTrue(yield* holds("scheduler:turn"));
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE orchestration_v2_projection_runs SET status = 'completed' WHERE run_id = 'run:report'`;
    assert.isFalse(yield* holds("scheduler:turn"));
    // Every bound Spectrum must release: a second, still active one holds the run.
    yield* bindSpectrum("scheduler:turn", { status: "active" });
    assert.isTrue(yield* holds("scheduler:turn"));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a report whose run ended before any provider turn holds, completed or not", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* ensureSpectraFixture;
    // Failed before the prompt reached the provider: startedAt is set, no provider turn, and
    // recovery concluded. The run's own timestamp never releases the fence.
    yield* receipt("prestart", "accepted");
    yield* run({
      id: "run:prestart",
      status: "failed",
      startedAt: true,
      userMessageId: "report-message:prestart",
    });
    yield* Recovery.writeRecoveryOutcome({
      sourceRunId: RunId.make("run:prestart"),
      threadId,
      status: "decided",
      outcome: "not_retryable",
      reason: "non_retryable",
    });
    yield* bindSpectrum("scheduler:prestart", { status: "settled", report: "prestart" });
    assert.isTrue(yield* holds("scheduler:prestart"));
    // Completed with no provider turn is no delivery either.
    yield* receipt("silent", "accepted");
    yield* run({
      id: "run:silent",
      status: "completed",
      startedAt: true,
      userMessageId: "report-message:silent",
    });
    yield* bindSpectrum("scheduler:silent", { status: "settled", report: "silent" });
    assert.isTrue(yield* holds("scheduler:silent"));
    // A pending provider turn never started.
    yield* providerTurn("run:silent", "pending");
    assert.isTrue(yield* holds("scheduler:silent"));
    // Abandoning an earlier report attempt does not release the current one.
    yield* bindSpectrum("scheduler:stale", {
      status: "settled",
      report: "silent",
      abandoned: "earlier",
    });
    assert.isTrue(yield* holds("scheduler:stale"));
    // The user abandoning this exact report releases it, for either run.
    yield* bindSpectrum("scheduler:abandoned", {
      status: "settled",
      report: "silent",
      abandoned: "silent",
    });
    assert.isFalse(yield* holds("scheduler:abandoned"));
    yield* bindSpectrum("scheduler:abandoned-prestart", {
      status: "settled",
      report: "prestart",
      abandoned: "prestart",
    });
    assert.isFalse(yield* holds("scheduler:abandoned-prestart"));
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
