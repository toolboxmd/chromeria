import {
  type CommandId,
  type MessageId,
  type RunId,
  type ScheduledTaskId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

import { ForkCommitGuardRejected } from "../childThreads/ForkCommitPlan.ts";
import { holdsTask } from "./state.ts";
import { listCheckStates } from "./store.ts";

/**
 * Exact run handoffs for scheduled tasks (toolboxmd/chromeria#174, D32/D35).
 * The scheduler follows a run only through links persisted on runs and the
 * conclusive recovery facts #169 records; it never guesses by thread or time.
 */

/** #169's conclusive fact for one failed source run (D35). */
export type RecoveryOutcome =
  | { readonly kind: "pending" }
  | { readonly kind: "retried"; readonly successorRunId: RunId }
  | { readonly kind: "not_retryable"; readonly reason: string }
  | { readonly kind: "abandoned"; readonly reason: string };

/**
 * #169's per-source reader (`readRecoveryOutcome`, approved as D35), wired
 * once its reviewed implementation merges. Null means this build records no
 * recovery facts, so a failed run has no automatic continuation to wait for.
 * A reader's null result is undecided and always holds.
 */
export class ScheduledRunRecovery extends Context.Reference<{
  readonly outcome:
    | ((sourceRunId: RunId) => Effect.Effect<RecoveryOutcome | null, SqlError, SqlClient.SqlClient>)
    | null;
}>("t3/scheduledTaskChecks/ScheduledRunRecovery", { defaultValue: () => ({ outcome: null }) }) {}

/** A Spectrum bound to a scheduler run, as #176's Spectrum state reports it (D32). */
export interface BoundSpectrum {
  readonly status: "active" | "settled" | "retired";
  /** The current report attempt's `message.dispatch`; null while none was written. */
  readonly report: {
    readonly commandId: CommandId;
    readonly messageId: MessageId;
    readonly threadId: ThreadId;
    /** Still in Spectrum's outbox, not yet dispatched. */
    readonly inOutbox: boolean;
  } | null;
  /** The user explicitly abandoned a report that never reached a provider. */
  readonly abandonedByUser: boolean;
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
  readonly started_at: string | null;
  readonly user_message_id: string | null;
  readonly restart_source: string | null;
  readonly prism_source: string | null;
}

const readRun = (sql: SqlClient.SqlClient, where: { readonly runId: string }) =>
  sql<RunRow>`
    SELECT run_id, status,
      json_extract(payload_json, '$.startedAt') AS started_at,
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
 */
export type RunEnd =
  | { readonly kind: "waiting" }
  | { readonly kind: "completed"; readonly runId: string }
  | { readonly kind: "ended"; readonly runId: string; readonly started: boolean };

export const followRun = Effect.fn("ScheduledTaskChecks.followRun")(function* (runId: string) {
  const sql = yield* SqlClient.SqlClient;
  const recovery = yield* ScheduledRunRecovery;
  let current = runId;
  // Any provider start along the chain means the provider saw the work.
  let started = false;
  for (let hop = 0; hop < MAX_HOPS; hop += 1) {
    const run = yield* readRun(sql, { runId: current });
    if (run === undefined) return { kind: "waiting" } satisfies RunEnd;
    started ||= run.started_at !== null;
    if (!TERMINAL.has(run.status)) return { kind: "waiting" } satisfies RunEnd;
    if (run.status === "completed") return { kind: "completed", runId: current } satisfies RunEnd;
    const restarted = yield* restartSuccessor(sql, current);
    if (restarted !== undefined) {
      current = restarted;
      continue;
    }
    if (recovery.outcome === null)
      return { kind: "ended", runId: current, started } satisfies RunEnd;
    const outcome = yield* recovery.outcome(current as RunId);
    if (outcome === null || outcome.kind === "pending") return { kind: "waiting" } satisfies RunEnd;
    if (outcome.kind === "retried") {
      // The fact names the successor; the successor must name this source back.
      const successor = yield* readRun(sql, { runId: outcome.successorRunId });
      if (successor === undefined || successor.prism_source !== current)
        return { kind: "waiting" } satisfies RunEnd;
      current = successor.run_id;
      continue;
    }
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
    if (end.kind === "ended" && !end.started && !spectrum.abandonedByUser) return true;
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
