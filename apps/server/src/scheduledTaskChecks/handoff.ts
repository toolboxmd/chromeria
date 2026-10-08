import {
  RunId,
  type CommandId,
  type MessageId,
  type ScheduledTaskId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

import { ForkCommitGuardRejected } from "../childThreads/ForkCommitPlan.ts";
import * as Recovery from "../prism/RecoveryHistory.ts";
import { holdsTask } from "./state.ts";
import { listCheckStates } from "./store.ts";

/**
 * Exact run handoffs for scheduled tasks (toolboxmd/chromeria#174, D32/D38).
 * The scheduler follows a run only through links persisted on runs and the
 * recovery state #169 records; it never guesses by thread or time.
 */

/** A Spectrum bound to a scheduler run, as #176's Spectrum state reports it (D32). */
export interface BoundSpectrum {
  readonly status: "active" | "settled" | "retired";
  /** The identity of the current report's `message.dispatch`; null while none was written. */
  readonly report: {
    readonly commandId: CommandId;
    readonly messageId: MessageId;
    readonly threadId: ThreadId;
    /** Still in Spectrum's outbox, not yet dispatched. */
    readonly inOutbox: boolean;
  } | null;
  /** The user's abandonment of a report; it counts only for the current report's command. */
  readonly reportAbandonment: { readonly commandId: CommandId } | null;
}

/** #176 provides the Spectrums bound to a scheduler run; without Spectrum there are none. */
export class ScheduledTaskSpectra extends Context.Reference<{
  readonly boundTo: (
    schedulerRunId: string,
  ) => Effect.Effect<ReadonlyArray<BoundSpectrum>, SqlError, SqlClient.SqlClient>;
}>("t3/scheduledTaskChecks/ScheduledTaskSpectra", {
  defaultValue: () => ({ boundTo: () => Effect.succeed([]) }),
}) {}

const TERMINAL = new Set(["completed", "failed", "interrupted", "cancelled", "rolled_back"]);
const MAX_HOPS = 32;

interface RunRow {
  readonly run_id: string;
  readonly status: string;
  readonly user_message_id: string | null;
  readonly restart_source: string | null;
  readonly prism_source: string | null;
}

const readRun = (sql: SqlClient.SqlClient, where: { readonly runId: string }) =>
  sql<RunRow>`
    SELECT run_id, status,
      json_extract(payload_json, '$.userMessageId') AS user_message_id,
      json_extract(payload_json, '$.restartContinuationOfRunId') AS restart_source,
      json_extract(payload_json, '$.forkPrismContinuationSourceRunId') AS prism_source
    FROM orchestration_v2_projection_runs WHERE run_id = ${where.runId}
  `.pipe(Effect.map((rows) => rows[0]));

const readRunByMessage = (sql: SqlClient.SqlClient, threadId: string, messageId: string) =>
  sql<{ readonly run_id: string }>`
    SELECT run_id FROM orchestration_v2_projection_runs
    WHERE thread_id = ${threadId}
      AND json_extract(payload_json, '$.userMessageId') = ${messageId}
    LIMIT 1
  `.pipe(Effect.map((rows) => rows[0]?.run_id));

/**
 * Whether any of these runs reached its provider, by upstream's delivery rule
 * (ProviderTurnStartService): a provider turn recorded for one of the run's
 * attempts. Adapters record it only as the provider takes the prompt; a run's
 * `startedAt` is set before delivery and a failed start records no turn. A
 * pending turn never started.
 */
export const reachedProvider = (sql: SqlClient.SqlClient, runIds: ReadonlyArray<string>) =>
  runIds.length === 0
    ? Effect.succeed(false)
    : sql`
        SELECT 1 FROM orchestration_v2_projection_provider_turns AS turn
        JOIN orchestration_v2_projection_run_attempts AS attempt
          ON attempt.attempt_id = turn.run_attempt_id
        WHERE attempt.run_id IN ${sql.in(runIds)}
          AND turn.status <> 'pending' AND turn.started_at IS NOT NULL
        LIMIT 1
      `.pipe(Effect.map((rows) => rows.length > 0));

/** Upstream's own restart continuation of a run, linked on the successor. */
const restartSuccessor = (sql: SqlClient.SqlClient, runId: string) =>
  sql<{ readonly run_id: string }>`
    SELECT run_id FROM orchestration_v2_projection_runs
    WHERE json_extract(payload_json, '$.restartContinuationOfRunId') = ${runId}
    LIMIT 1
  `.pipe(Effect.map((rows) => rows[0]?.run_id));

/**
 * Where a run's work stands after following its exact continuations:
 * - `waiting`: it or a continuation is not finished, or recovery is undecided;
 * - `completed`: it or an exact continuation completed;
 * - `ended`: it ended without completing and nothing will continue it.
 *
 * `started` says whether the provider took any run along the chain; a terminal
 * status alone, completed included, is no proof of delivery.
 */
export type RunEnd =
  | { readonly kind: "waiting" }
  | { readonly kind: "completed"; readonly runId: string; readonly started: boolean }
  | { readonly kind: "ended"; readonly runId: string; readonly started: boolean };

export const followRun = Effect.fn("ScheduledTaskChecks.followRun")(function* (runId: string) {
  const sql = yield* SqlClient.SqlClient;
  let current = runId;
  let started = false;
  for (let hop = 0; hop < MAX_HOPS; hop += 1) {
    const run = yield* readRun(sql, { runId: current });
    if (run === undefined) return { kind: "waiting" } satisfies RunEnd;
    if (!TERMINAL.has(run.status)) return { kind: "waiting" } satisfies RunEnd;
    started ||= yield* reachedProvider(sql, [current]);
    if (run.status === "completed")
      return { kind: "completed", runId: current, started } satisfies RunEnd;
    const restarted = yield* restartSuccessor(sql, current);
    if (restarted !== undefined) {
      current = restarted;
      continue;
    }
    // #169's recovery state for this run (D38). An admitted continuation, linked
    // from the successor run itself, is followed first; the index admits at most
    // one, so more is never chosen between. Pending recovery, such as a
    // re-enabled reset, holds even over a recorded opt-out, as does no decision.
    const recovery = yield* Recovery.readRecoveryState(RunId.make(current));
    const [admitted, ...more] = recovery.admittedContinuations;
    if (admitted !== undefined) {
      if (more.length > 0) return { kind: "waiting" } satisfies RunEnd;
      current = admitted.successorRunId;
      continue;
    }
    const decision = recovery.decision;
    if (recovery.pendingRecovery !== null || decision === null || decision.status === "pending")
      return { kind: "waiting" } satisfies RunEnd;
    // A retry is admitted with its run, so a decided retry without one is not done yet.
    if (decision.outcome === "retried") return { kind: "waiting" } satisfies RunEnd;
    // Not retryable, or recovery abandoned it on its own: the scheduler decides now.
    return { kind: "ended", runId: current, started } satisfies RunEnd;
  }
  return { kind: "waiting" } satisfies RunEnd;
});

/** Where the run started by one scheduler send stands; waiting until that send has a run. */
export const followSend = Effect.fn("ScheduledTaskChecks.followSend")(function* (
  threadId: ThreadId,
  messageId: MessageId,
) {
  const sql = yield* SqlClient.SqlClient;
  const runId = yield* readRunByMessage(sql, threadId, messageId);
  return runId === undefined ? ({ kind: "waiting" } satisfies RunEnd) : yield* followRun(runId);
});

/**
 * The completion fence (D32): whether any Spectrum bound to the scheduler run
 * still holds it. A run settles only when every bound Spectrum settled and its
 * report's turn ended; the caller reruns its check afterwards.
 */
export const reportsHold = Effect.fn("ScheduledTaskChecks.reportsHold")(function* (
  schedulerRunId: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const spectra = yield* ScheduledTaskSpectra;
  for (const spectrum of yield* spectra.boundTo(schedulerRunId)) {
    if (spectrum.status === "active") return true;
    const report = spectrum.report;
    if (report === null) {
      if (spectrum.status === "retired") continue;
      return true;
    }
    if (report.inOutbox) return true;
    const receipts = yield* sql<{ readonly status: string }>`
      SELECT status FROM orchestration_v2_command_receipts WHERE command_id = ${report.commandId}
    `;
    // No receipt yet, or a rejected one: Spectrum still owes this or a new attempt.
    if (receipts[0]?.status !== "accepted") return true;
    const end = yield* followSend(report.threadId, report.messageId);
    if (end.kind === "waiting") return true;
    // A report that never reached a provider, however its run ended, holds
    // until a new attempt or the user abandons this exact report.
    if (!end.started && spectrum.reportAbandonment?.commandId !== report.commandId) return true;
  }
  return false;
});

/**
 * Exact binding at Spectrum start (D32): the scheduler run whose recorded send
 * started the caller's run, following the caller back through persisted
 * continuation links only. Null when the caller is not a scheduler run's work.
 */
export const resolveSchedulerRun = Effect.fn("ScheduledTaskChecks.resolveSchedulerRun")(
  function* (input: { readonly callerThreadId: ThreadId; readonly callerRunId: RunId }) {
    const sql = yield* SqlClient.SqlClient;
    let current: string = input.callerRunId;
    let origin: RunRow | undefined;
    for (let hop = 0; hop < MAX_HOPS && origin === undefined; hop += 1) {
      const run = yield* readRun(sql, { runId: current });
      if (run === undefined) return null;
      const source = run.restart_source ?? run.prism_source;
      if (source === null) origin = run;
      else current = source;
    }
    if (origin?.user_message_id == null) return null;
    for (const state of yield* listCheckStates()) {
      for (const run of state.runs) {
        if (!holdsTask(state, run) || run.threadId !== input.callerThreadId) continue;
        if (run.sends.some((send) => send.messageId === origin.user_message_id))
          return { scheduledTaskId: state.taskId, schedulerRunId: run.id } as const;
      }
    }
    return null;
  },
);

/**
 * A guard for Spectrum's registration commit plan: the scheduler run must
 * still be unfinished and bound to the caller's thread. Spectrum registers
 * unbound when it rejects.
 */
export const schedulerRunOpenGuard = (input: {
  readonly scheduledTaskId: ScheduledTaskId;
  readonly schedulerRunId: string;
  readonly callerThreadId: ThreadId;
}) =>
  Effect.gen(function* () {
    const states = yield* listCheckStates();
    const state = states.find((entry) => entry.taskId === input.scheduledTaskId);
    const run = state?.runs.find((entry) => entry.id === input.schedulerRunId);
    if (
      state === undefined ||
      run === undefined ||
      !holdsTask(state, run) ||
      run.threadId !== input.callerThreadId
    )
      return yield* new ForkCommitGuardRejected({
        threadId: input.callerThreadId,
        kind: "state_conflict",
      });
  }).pipe(
    Effect.catchTags({
      SqlError: () =>
        Effect.fail(
          new ForkCommitGuardRejected({ threadId: input.callerThreadId, kind: "storage_failure" }),
        ),
      SchemaError: () =>
        Effect.fail(
          new ForkCommitGuardRejected({ threadId: input.callerThreadId, kind: "storage_failure" }),
        ),
    }),
  );
