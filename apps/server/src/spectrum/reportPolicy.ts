import {
  CommandId,
  MessageId,
  OrchestrationV2AppThreadJson,
  OrchestrationV2Command,
  OrchestrationV2ProviderFailure,
  OrchestrationV2RunJson,
  OrchestrationV2TurnItemJson,
  type OrchestrationV2Run,
  type RunId,
  type ThreadId,
} from "@t3tools/contracts";
import { latestRootProviderFailure } from "@t3tools/shared/orchestrationV2ThreadError";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

import { ForkCommitGuardRejected, type ForkCommitPlan } from "../childThreads/ForkCommitPlan.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as Recovery from "../prism/RecoveryHistory.ts";
import { followSend } from "../scheduledTaskChecks/handoff.ts";
import { SpectrumState, type SpectrumMutation } from "./state.ts";
import {
  applySpectrumMutation,
  checkSpectrumMutation,
  readSpectrum,
  type SpectrumMutationRejected,
} from "./store.ts";

/**
 * Report delivery (D40, D42) is bounded at-least-once, not exactly-once: an
 * attempt can process the report, run tools, then fail before its turn
 * completes, and the next attempt processes the same text again. Spectrum
 * accepts that duplicate risk for three total attempts, and never retries after
 * Stop, abandonment, a retired Spectrum or a failure without typed retryable
 * evidence.
 */
export const REPORT_ATTEMPTS = 3;

export type ReportCommand = NonNullable<SpectrumState["report"]>;
export interface ReportAttempt {
  readonly generation: number;
  readonly cycle: number;
  readonly ordinal: number;
}

const sameCommand = Schema.toEquivalence(OrchestrationV2Command);
const sameOutcome = Schema.toEquivalence(Schema.NullOr(Recovery.RecoveryOutcome));
const sameState = Schema.toEquivalence(SpectrumState);
const sameFailure = Schema.toEquivalence(Schema.NullOr(OrchestrationV2ProviderFailure));

/** Each attempt has its own command and message, with the same text, for the caller's thread. */
export function reportCommand(
  state: SpectrumState,
  attempt: ReportAttempt,
  text: string,
): ReportCommand {
  const commandId = CommandId.make(
    `spectrum:${state.threadId}:${attempt.generation}:${attempt.cycle}:report:${attempt.ordinal}`,
  );
  return {
    type: "message.dispatch",
    commandId,
    messageId: MessageId.make(`${commandId}:message`),
    threadId: state.callerThreadId,
    senderThreadId: state.threadId,
    createdBy: "system",
    creationSource: "server",
    attachments: [],
    dispatchMode: { type: "queue_after_active" },
    text,
  };
}

const ATTEMPT = /^(0|[1-9]\d*):(0|[1-9]\d*):report:([1-9]\d*)$/;

/** The current report's attempt, or null for a report this module did not write, which holds for a person. */
export function reportAttempt(state: SpectrumState): ReportAttempt | null {
  const report = state.report;
  const prefix = `spectrum:${state.threadId}:`;
  if (report === null || !report.commandId.startsWith(prefix)) return null;
  const match = ATTEMPT.exec(report.commandId.slice(prefix.length));
  if (match === null) return null;
  const attempt = {
    generation: Number(match[1]),
    cycle: Number(match[2]),
    ordinal: Number(match[3]),
  };
  if (attempt.ordinal > REPORT_ATTEMPTS) return null;
  return sameCommand(report, reportCommand(state, attempt, report.text)) ? attempt : null;
}

/** Every attempt of the current report, first to current; only the current one for an unknown report. */
export function reportAttempts(state: SpectrumState): ReadonlyArray<ReportCommand> {
  const report = state.report;
  if (report === null) return [];
  const attempt = reportAttempt(state);
  if (attempt === null) return [report];
  return Array.from({ length: attempt.ordinal }, (_, index) =>
    reportCommand(state, { ...attempt, ordinal: index + 1 }, report.text),
  );
}

/**
 * The first attempt, written into the outbox after any pending cancellation.
 * Pure and revision-neutral, so settle and the controller's Stop plan commit it
 * with their own revision. Callers decide whether an existing report may be replaced.
 */
export function makeInitialReport(
  state: SpectrumState,
  text: string,
  retired: boolean,
): SpectrumState {
  const report = reportCommand(
    state,
    { generation: state.generation, cycle: state.cycle, ordinal: 1 },
    text,
  );
  return {
    ...state,
    status: retired ? "retired" : "settled",
    report,
    reportAbandonment: null,
    outbox: [...state.outbox, report],
  };
}

/** Stop drops a report attempt that was never sent. Revision-neutral, like makeInitialReport. */
export function dropUnsentReport(state: SpectrumState): SpectrumState {
  const report = state.report;
  return report === null
    ? state
    : {
        ...state,
        outbox: state.outbox.filter((command) => command.commandId !== report.commandId),
      };
}

/**
 * Rebuilt from durable data for every send and replay: only the current,
 * unabandoned attempt in the outbox. A retired transcript does not refuse a
 * Stop notice; the orchestrator's own admission still checks the caller.
 */
export function reportAdmission(threadId: ThreadId, report: ReportCommand): ForkCommitPlan {
  return {
    guards: [
      Effect.gen(function* () {
        const state = Option.getOrNull(yield* readSpectrum(threadId));
        if (
          state?.report == null ||
          !sameCommand(state.report, report) ||
          state.reportAbandonment?.commandId === report.commandId ||
          !state.outbox.some(
            (pending) =>
              pending.type !== "spectrum.transcript.append" && sameCommand(pending, report),
          )
        )
          return yield* new ForkCommitGuardRejected({ threadId, kind: "state_conflict" });
      }).pipe(Effect.mapError((error) => guardError(threadId, error))),
    ],
    mutations: [],
  };
}

const isGuardRejection = Schema.is(ForkCommitGuardRejected);
const guardError = (threadId: ThreadId, error: unknown) =>
  isGuardRejection(error)
    ? error
    : new ForkCommitGuardRejected({ threadId, kind: "storage_failure" });

const storeRejection = (threadId: ThreadId) => ({
  SpectrumMutationRejected: (error: SpectrumMutationRejected) =>
    new ForkCommitGuardRejected({
      threadId,
      kind: error.kind === "invalid-transition" ? "invalid_transition" : "state_conflict",
    }),
  SpectrumStoreError: () => new ForkCommitGuardRejected({ threadId, kind: "storage_failure" }),
});

/** Everything but the report fields, the revision and report commands in the outbox. */
function reportOnly(current: SpectrumState, next: SpectrumState): boolean {
  const reports = new Set([current.report?.commandId, next.report?.commandId]);
  const rest = (state: SpectrumState): SpectrumState => ({
    ...state,
    revision: 0,
    report: null,
    reportAbandonment: null,
    outbox: state.outbox.filter((command) => !reports.has(command.commandId)),
  });
  return sameState(rest(current), rest(next));
}

/**
 * Commits report bookkeeping (sent, retried, abandoned) on its own: unlike the
 * transcript's plan it has no thread guards, so a Stop notice and its
 * abandonment still commit after the transcript retired. It may change nothing
 * outside the report fields and the report's own outbox entries.
 */
export function reportStatePlan(mutation: SpectrumMutation): ForkCommitPlan {
  const threadId = mutation.state.threadId;
  return {
    guards: [
      Effect.gen(function* () {
        yield* checkSpectrumMutation(mutation);
        const current = Option.getOrNull(yield* readSpectrum(threadId));
        if (current === null || !reportOnly(current, mutation.state))
          return yield* new ForkCommitGuardRejected({ threadId, kind: "invalid_transition" });
      }).pipe(Effect.catchTags(storeRejection(threadId))),
    ],
    mutations: [applySpectrumMutation(mutation).pipe(Effect.catchTags(storeRejection(threadId)))],
  };
}

export const REPORT_STOPPED = "The Spectrum report was stopped before its turn completed.";
export const REPORT_REJECTED = "The caller's thread refused the Spectrum report.";
export const REPORT_NOT_RETRYABLE =
  "The Spectrum report's turn failed and cannot be retried automatically.";
export const REPORT_EXHAUSTED = `The Spectrum report's turn failed ${REPORT_ATTEMPTS} times.`;

/**
 * The exact recorded chain of one attempt: the run its message started, then
 * each restart or Prism continuation linked on the successor run. Planning
 * captures it; commit guards compare it with projection and fork rows only.
 */
export interface ReportChain {
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
  readonly runIds: ReadonlyArray<RunId>;
  readonly tailStatus: OrchestrationV2Run["status"];
  readonly stopped: boolean;
  /** The tail's typed root failure, which alone can make a failure retryable. */
  readonly failure: OrchestrationV2ProviderFailure | null;
  readonly decision: Recovery.RecoveryOutcome | null;
  readonly limitRecovery: {
    readonly resetAt: string | null;
    readonly autoResume: boolean;
  } | null;
}

/**
 * Where one attempt stands. `open` and `held` may still run; the others are
 * conclusive, though `retryable` still needs the bound and a live Spectrum.
 */
export type AttemptOutcome =
  | { readonly kind: "unsent" }
  | { readonly kind: "held" }
  | {
      readonly kind: "open";
      readonly runIds: ReadonlyArray<RunId>;
      readonly tail: OrchestrationV2Run;
    }
  | { readonly kind: "delivered"; readonly chain: ReportChain }
  | { readonly kind: "retryable"; readonly chain: ReportChain }
  | { readonly kind: "needs-you"; readonly reason: string; readonly chain: ReportChain | null };

const retryableFailure = (failure: OrchestrationV2ProviderFailure | null) =>
  failure?.retryable === true && failure.class !== "usage_limit";

const TERMINAL = new Set(["completed", "failed", "interrupted", "cancelled", "rolled_back"]);
const MAX_HOPS = 32;
const RESTART_CANCEL = "command:runtime-reconcile:";
// Prism concluded without a retry, so only the failure's own typed evidence can allow one.
const UNJUDGED = new Set(["exhausted", "non_mcp", "superseded"]);
const decodeRun = Schema.decodeUnknownSync(Schema.fromJsonString(OrchestrationV2RunJson));
const decodeThread = Schema.decodeUnknownSync(Schema.fromJsonString(OrchestrationV2AppThreadJson));
const decodeItem = Schema.decodeUnknownSync(Schema.fromJsonString(OrchestrationV2TurnItemJson));

const decodeRuns = (rows: ReadonlyArray<{ readonly payload_json: string }>) =>
  rows.map((row) => decodeRun(row.payload_json));

/** Root then single successors; null when a message or run has more than one, which never resolves alone. */
const walkChain = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  threadId: ThreadId,
  messageId: MessageId,
) {
  const roots = decodeRuns(
    yield* sql<{ readonly payload_json: string }>`
      SELECT payload_json FROM orchestration_v2_projection_runs
      WHERE thread_id = ${threadId} AND json_extract(payload_json, '$.userMessageId') = ${messageId}
    `,
  );
  if (roots.length !== 1) return roots.length === 0 ? [] : null;
  const chain = [roots[0]!];
  for (let hop = 0; hop < MAX_HOPS; hop += 1) {
    const tail = chain.at(-1)!;
    const next = decodeRuns(
      yield* sql<{ readonly payload_json: string }>`
        SELECT payload_json FROM orchestration_v2_projection_runs
        WHERE thread_id = ${threadId}
          AND (json_extract(payload_json, '$.restartContinuationOfRunId') = ${tail.id}
            OR json_extract(payload_json, '$.forkPrismContinuationSourceRunId') = ${tail.id})
      `,
    );
    if (next.length === 0) return chain;
    if (next.length > 1) return null;
    chain.push(next[0]!);
  }
  return null;
});

/**
 * Classifies one attempt from durable receipts, its exact run chain, #169's
 * recovery history and #174's followSend. Missing or undecided evidence holds;
 * nothing is concluded from absence. Reads through the caller's SqlClient, so
 * the scheduler can call it inside its transaction; never inside a commit guard.
 */
export const attemptOutcome = Effect.fn("SpectrumReport.attemptOutcome")(function* (
  threadId: ThreadId,
  attempt: { readonly commandId: CommandId; readonly messageId: MessageId },
): Effect.fn.Return<AttemptOutcome, SqlError, SqlClient.SqlClient> {
  const sql = yield* SqlClient.SqlClient;
  const receipts = yield* sql<{ readonly status: string }>`
    SELECT status FROM orchestration_command_receipts WHERE command_id = ${attempt.commandId}
  `;
  if (receipts[0] === undefined) return { kind: "unsent" };
  // A rejection carries no typed failure, so it never earns a retry.
  if (receipts[0].status !== "accepted")
    return { kind: "needs-you", reason: REPORT_REJECTED, chain: null };
  const runs = yield* walkChain(sql, threadId, attempt.messageId);
  if (runs === null || runs.length === 0) return { kind: "held" };
  const runIds = runs.map((run) => run.id);
  const tail = runs.at(-1)!;
  if (!TERMINAL.has(tail.status)) return { kind: "open", runIds, tail };
  const items = (yield* sql<{ readonly payload_json: string }>`
    SELECT payload_json FROM orchestration_v2_projection_turn_items
    WHERE thread_id = ${threadId} AND run_id = ${tail.id}
      AND type IN ('run_interrupt_request', 'error')
  `).map((row) => decodeItem(row.payload_json));
  const stopped = items.some((item) => item.type === "run_interrupt_request");
  const failure = latestRootProviderFailure(tail, items);
  const recovery = yield* Recovery.readRecoveryState(tail.id);
  const threads = yield* sql<{ readonly payload_json: string }>`
    SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}
  `;
  const limit =
    threads[0] === undefined ? undefined : decodeThread(threads[0].payload_json).limitRecovery;
  const chain: ReportChain = {
    threadId,
    messageId: attempt.messageId,
    runIds,
    tailStatus: tail.status,
    stopped,
    failure,
    decision: recovery.decision,
    limitRecovery:
      limit?.runId === tail.id
        ? { resetAt: limit.resetAt ?? null, autoResume: limit.autoResume }
        : null,
  };
  const end = yield* followSend(threadId, attempt.messageId);
  if (tail.status === "completed")
    return end.kind === "completed" && end.runId === tail.id
      ? { kind: "delivered", chain }
      : { kind: "held" };
  if (tail.status === "cancelled") {
    const cancelledBy = yield* sql<{ readonly command_id: string | null }>`
      SELECT command_id FROM orchestration_events
      WHERE aggregate_kind = 'thread' AND stream_id = ${threadId} AND event_type = 'run.updated'
        AND json_extract(payload_json, '$.id') = ${tail.id}
        AND json_extract(payload_json, '$.status') = 'cancelled'
      ORDER BY sequence LIMIT 1
    `;
    const command = cancelledBy[0]?.command_id;
    // Restart reconciliation comes first: a continuation may still follow, so its successor stays unresolved.
    if (command == null || command.startsWith(RESTART_CANCEL)) return { kind: "held" };
  }
  // Explicit Stop on the exact tail, or any other cancellation without a recovery decision.
  if (stopped || (tail.status === "cancelled" && recovery.decision === null))
    return { kind: "needs-you", reason: REPORT_STOPPED, chain };
  if (end.kind !== "ended" || end.runId !== tail.id) return { kind: "held" };
  const decision = recovery.decision;
  if (decision?.status !== "decided" || decision.outcome === "retried") return { kind: "held" };
  if (decision.outcome === "abandoned")
    return {
      kind: "needs-you",
      reason: decision.reason === "ineligible" ? REPORT_NOT_RETRYABLE : REPORT_STOPPED,
      chain,
    };
  if (UNJUDGED.has(decision.reason) && retryableFailure(failure))
    return { kind: "retryable", chain };
  return { kind: "needs-you", reason: REPORT_NOT_RETRYABLE, chain };
});

/** What the scheduler shows (D40): needs-you only for the current attempt and only on conclusive evidence. */
export const reportNeedsYou = Effect.fn("SpectrumReport.needsYou")(function* (
  state: SpectrumState,
): Effect.fn.Return<
  { readonly commandId: CommandId; readonly reason: string } | null,
  SqlError,
  SqlClient.SqlClient
> {
  const report = state.report;
  const attempt = reportAttempt(state);
  if (report === null || attempt === null) return null;
  if (state.outbox.some((command) => command.commandId === report.commandId)) return null;
  const outcome = yield* attemptOutcome(state.callerThreadId, report);
  // Stop dropped this attempt before it was sent; nothing will send it now.
  if (outcome.kind === "unsent")
    return state.status === "retired"
      ? { commandId: report.commandId, reason: REPORT_STOPPED }
      : null;
  if (outcome.kind === "needs-you") return { commandId: report.commandId, reason: outcome.reason };
  if (outcome.kind !== "retryable") return null;
  if (state.status === "retired") return { commandId: report.commandId, reason: REPORT_STOPPED };
  return attempt.ordinal >= REPORT_ATTEMPTS
    ? { commandId: report.commandId, reason: REPORT_EXHAUSTED }
    : null;
});

/** Planning for a retry: every attempt so far concluded retryable, with the chains that prove it. */
export const retryEvidence = Effect.fn("SpectrumReport.retryEvidence")(function* (
  state: SpectrumState,
): Effect.fn.Return<ReadonlyArray<ReportChain> | null, SqlError, SqlClient.SqlClient> {
  const attempt = reportAttempt(state);
  const report = state.report;
  if (
    report === null ||
    attempt === null ||
    attempt.ordinal >= REPORT_ATTEMPTS ||
    state.status === "retired" ||
    state.reportAbandonment?.commandId === report.commandId ||
    state.outbox.some((command) => command.commandId === report.commandId)
  )
    return null;
  const chains: Array<ReportChain> = [];
  for (const command of reportAttempts(state)) {
    const outcome = yield* attemptOutcome(state.callerThreadId, command);
    if (outcome.kind !== "retryable") return null;
    chains.push(outcome.chain);
  }
  return chains;
});

/** The retried state: a new attempt with new identities and the same text, appended to the outbox. */
export function retriedState(state: SpectrumState): SpectrumState {
  const attempt = reportAttempt(state);
  if (state.report === null || attempt === null)
    throw new RangeError("Only a known report attempt can be retried.");
  const report = reportCommand(
    state,
    { ...attempt, ordinal: attempt.ordinal + 1 },
    state.report.text,
  );
  return { ...state, revision: state.revision + 1, report, outbox: [...state.outbox, report] };
}

/** Rechecks captured chains inside a commit with projection and fork rows only; any change rejects. */
export function reportChainsUnchanged(
  threadId: ThreadId,
  chains: ReadonlyArray<ReportChain>,
): ForkCommitPlan {
  return {
    guards: [
      Effect.forEach(chains, (chain) => chainUnchanged(chain), { discard: true }).pipe(
        Effect.mapError((error) => guardError(threadId, error)),
      ),
    ],
    mutations: [],
  };
}

const conflict = (threadId: ThreadId) =>
  new ForkCommitGuardRejected({ threadId, kind: "state_conflict" });

/** The captured root and links still hold; links are immutable once a successor run is written. */
const linksHold = (
  runs: ReadonlyArray<OrchestrationV2Run>,
  chain: Pick<ReportChain, "messageId" | "runIds">,
) => {
  const roots = runs.filter((run) => run.userMessageId === chain.messageId);
  if (roots.length !== 1 || roots[0]!.id !== chain.runIds[0]) return false;
  return chain.runIds.every((runId, index) => {
    const run = runs.find((candidate) => candidate.id === runId);
    const source = index === 0 ? undefined : chain.runIds[index - 1];
    return (
      run !== undefined &&
      (source === undefined ||
        run.restartContinuationOfRunId === source ||
        run.forkPrismContinuationSourceRunId === source)
    );
  });
};

const chainUnchanged = Effect.fnUntraced(function* (chain: ReportChain) {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sql = yield* SqlClient.SqlClient;
  const tailId = chain.runIds.at(-1);
  if (tailId === undefined) return yield* conflict(chain.threadId);
  const records = yield* projections.getThreadRecords(chain.threadId, ["runs", "turnItems"], {
    turnItemRunIds: [tailId],
    turnItemTypes: ["run_interrupt_request", "error"],
  });
  const tail = records.runs.find((run) => run.id === tailId);
  if (
    tail === undefined ||
    tail.status !== chain.tailStatus ||
    !linksHold(records.runs, chain) ||
    records.runs.some(
      (run) =>
        run.restartContinuationOfRunId === tailId ||
        run.forkPrismContinuationSourceRunId === tailId,
    )
  )
    return yield* conflict(chain.threadId);
  const items = records.turnItems.filter((item) => item.runId === tailId);
  const limit = records.thread.limitRecovery;
  const limitRecovery =
    limit?.runId === tailId
      ? { resetAt: limit.resetAt ?? null, autoResume: limit.autoResume }
      : null;
  if (
    items.some((item) => item.type === "run_interrupt_request") !== chain.stopped ||
    !sameFailure(latestRootProviderFailure(tail, items), chain.failure) ||
    limitRecovery?.resetAt !== chain.limitRecovery?.resetAt ||
    limitRecovery?.autoResume !== chain.limitRecovery?.autoResume ||
    !sameOutcome(yield* Recovery.readRecoveryOutcome(tailId), chain.decision)
  )
    return yield* conflict(chain.threadId);
  const controllers = yield* sql`SELECT 1 FROM fork_prism_recovery
    WHERE source_run_id = ${tailId} AND state IN ('retry_pending', 'retry_started', 'reset_wait')
    LIMIT 1`;
  if (controllers.length > 0) return yield* conflict(chain.threadId);
});

/** Proof that an abandoned report can no longer run; the controller's reopen commits it as a guard. */
export interface ReportDrainProof {
  readonly threadId: ThreadId;
  readonly commandId: CommandId;
  readonly chains: ReadonlyArray<ReportChain>;
}

/**
 * Planning for reopen (D42): null while any attempt of the abandoned report is
 * open, held or unresolved, such as a restart-cancelled run that may still be
 * continued, or while a terminal run's provider work still drains: an
 * unsettled attempt, provider turn or effect, including a pending start that a
 * late queue release could still run. An attempt never sent stays unsent:
 * admission refuses it once abandoned or replaced, so it holds no chain.
 */
export const reportDrainProof = Effect.fn("SpectrumReport.drainProof")(function* (
  state: SpectrumState,
): Effect.fn.Return<ReportDrainProof | null, SqlError, SqlClient.SqlClient> {
  const report = state.report;
  if (
    report === null ||
    state.reportAbandonment?.commandId !== report.commandId ||
    state.outbox.some((command) => command.commandId === report.commandId)
  )
    return null;
  const chains: Array<ReportChain> = [];
  for (const command of reportAttempts(state)) {
    const outcome = yield* attemptOutcome(state.callerThreadId, command);
    if (outcome.kind === "unsent") continue;
    if (outcome.kind === "open" || outcome.kind === "held") return null;
    if (outcome.chain === null) continue;
    if (yield* stillDraining(outcome.chain, command.commandId)) return null;
    chains.push(outcome.chain);
  }
  return { threadId: state.threadId, commandId: report.commandId, chains };
});

const UNSETTLED = ["pending", "running"];

/** Provider work still owed by a chain whose runs are already terminal in the projection. */
const stillDraining = Effect.fnUntraced(function* (chain: ReportChain, commandId: CommandId) {
  const sql = yield* SqlClient.SqlClient;
  const attempts = sql`SELECT attempt_id FROM orchestration_v2_projection_run_attempts
    WHERE thread_id = ${chain.threadId} AND run_id IN ${sql.in(chain.runIds)}`;
  const turns = sql`SELECT provider_turn_id FROM orchestration_v2_projection_provider_turns
    WHERE run_attempt_id IN (${attempts})`;
  const rows = yield* sql`
    SELECT 1 FROM orchestration_v2_projection_run_attempts
    WHERE thread_id = ${chain.threadId} AND run_id IN ${sql.in(chain.runIds)}
      AND status IN ${sql.in(UNSETTLED)}
    UNION ALL
    SELECT 1 FROM orchestration_v2_projection_provider_turns
    WHERE run_attempt_id IN (${attempts}) AND status IN ${sql.in(UNSETTLED)}
    UNION ALL
    SELECT 1 FROM orchestration_v2_effect_outbox
    WHERE thread_id = ${chain.threadId} AND status IN ${sql.in(UNSETTLED)}
      AND (json_extract(payload_json, '$.runId') IN ${sql.in(chain.runIds)}
        OR json_extract(payload_json, '$.providerTurnId') IN (${turns})
        OR substr(command_id, 1, ${commandId.length}) = ${commandId})
    LIMIT 1
  `;
  return rows.length > 0;
});

/** The reopen guard: the same abandonment is current and every captured chain is unchanged. */
export function reportDrainedPlan(proof: ReportDrainProof): ForkCommitPlan {
  return {
    guards: [
      Effect.gen(function* () {
        const state = Option.getOrNull(yield* readSpectrum(proof.threadId));
        if (
          state?.report?.commandId !== proof.commandId ||
          state.reportAbandonment?.commandId !== proof.commandId ||
          state.outbox.some((command) => command.commandId === proof.commandId)
        )
          return yield* conflict(proof.threadId);
        // Effects are planning-time evidence only (D14); a settled terminal run creates none later.
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        for (const chain of proof.chains) {
          const records = yield* projections.getThreadRecords(chain.threadId, [
            "attempts",
            "providerTurns",
          ]);
          const attempts = records.attempts.filter((attempt) =>
            chain.runIds.includes(attempt.runId),
          );
          if (
            attempts.some((attempt) => UNSETTLED.includes(attempt.status)) ||
            records.providerTurns.some(
              (turn) =>
                UNSETTLED.includes(turn.status) &&
                attempts.some((attempt) => attempt.id === turn.runAttemptId),
            )
          )
            return yield* conflict(proof.threadId);
        }
      }).pipe(Effect.mapError((error) => guardError(proof.threadId, error))),
      ...reportChainsUnchanged(proof.threadId, proof.chains).guards,
    ],
    mutations: [],
  };
}

/** A cancellation of the live run of an abandoned report's exact chain. */
export function reportCancellation(
  report: ReportCommand,
  tail: OrchestrationV2Run,
): Extract<OrchestrationV2Command, { readonly type: "run.interrupt" | "queued-run.cancel" }> {
  const commandId = CommandId.make(`${report.commandId}:cancel:${tail.id}:${tail.status}`);
  return tail.status === "queued"
    ? { type: "queued-run.cancel", commandId, threadId: tail.threadId, runId: tail.id }
    : { type: "run.interrupt", commandId, threadId: tail.threadId, runId: tail.id };
}

/** Admits a cancellation only for the abandoned current report and a run on its recorded chain. */
export function reportCancelAdmission(
  threadId: ThreadId,
  report: ReportCommand,
  chain: Pick<ReportChain, "messageId" | "runIds">,
): ForkCommitPlan {
  return {
    guards: [
      Effect.gen(function* () {
        const state = Option.getOrNull(yield* readSpectrum(threadId));
        if (
          state?.report?.commandId !== report.commandId ||
          state.reportAbandonment?.commandId !== report.commandId
        )
          return yield* conflict(threadId);
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const records = yield* projections.getThreadRecords(report.threadId, ["runs"]);
        if (!linksHold(records.runs, chain)) return yield* conflict(threadId);
      }).pipe(Effect.mapError((error) => guardError(threadId, error))),
    ],
    mutations: [],
  };
}
