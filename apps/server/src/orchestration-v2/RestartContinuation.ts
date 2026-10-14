import { runRanAfter } from "@t3tools/shared/orchestrationV2ThreadError";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  CommandId,
  latestProviderTurnForAttempt,
  MessageId,
  type OrchestrationV2Run,
  type RunId,
  type ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import type { ProjectionRuntimeRecoveryState } from "./ProjectionStore.ts";
import { RESTART_CUT_REASONS, type RestartCutReason } from "@t3tools/shared/restartCut";

import * as ServerSettings from "../serverSettings.ts";
import { isNativeMaintenanceCommand } from "./CommandPolicy.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import {
  awaitingProviderThreadId,
  cancelledRosterTaskWork,
  cancelledTurnItemWork,
  isAwaitedWork,
  isRestartContinuationSource,
  cancelledBeforeStart,
  isRestartNoteSource,
  neverStarted,
  restartCancelledBackgroundWorkNote,
  restartContinuationNote,
} from "./RestartBackgroundNote.ts";

const CONTINUE_PROMPT = "Continue where you left off.";

const MAX_CONTINUATION_CHAIN = 3;
const CONTINUATION_LOOP_WINDOW_MS = 5 * 60 * 1_000;

function restartContinuationBlocked(
  projection: Pick<
    ProjectionRuntimeRecoveryState,
    "thread" | "runtimeRequests" | "providerTurns" | "turnItems"
  >,
  source: OrchestrationV2Run,
  now: DateTime.Utc,
  settled: boolean,
): boolean {
  const { thread } = projection;
  if (
    thread.archivedAt !== null ||
    thread.deletedAt !== null ||
    thread.settledOverride === "settled" ||
    (thread.snoozedUntil != null && DateTime.isGreaterThan(thread.snoozedUntil, now))
  )
    return true;
  const turn = latestProviderTurnForAttempt(projection.providerTurns, source.activeAttemptId);
  if (
    !settled &&
    turn !== undefined &&
    (turn.status === "completed" || turn.status === "failed" || turn.status === "interrupted")
  )
    return true;
  const waitingRequest = projection.runtimeRequests.some(
    (request) =>
      request.kind !== "dynamic_tool_call" &&
      request.kind !== "auth_refresh" &&
      request.responseCapability.type !== "message" &&
      (request.status === "pending" ||
        ((request.status === "expired" || request.status === "cancelled") &&
          request.responseCapability.type === "not_resumable" &&
          turn !== undefined &&
          turn.id === request.providerTurnId)),
  );
  if (waitingRequest) return true;
  return projection.turnItems.some(
    (item) =>
      item.type === "secret_request" &&
      item.runId === source.id &&
      (item.secretStatus === "pending" ||
        (item.secretStatus === "cancelled" &&
          item.completedAt !== null &&
          source.completedAt !== null &&
          DateTime.Equivalence(item.completedAt, source.completedAt))),
  );
}

/** App-owned delegations continue in their own threads and report back. */
function hasOpenAwaitedWork(
  projection: Pick<ProjectionRuntimeRecoveryState, "runs" | "turnItems">,
  providerThread: ProjectionRuntimeRecoveryState["providerThreads"][number],
): boolean {
  const openItem = projection.turnItems.some((item) => {
    if (item.status !== "pending" && item.status !== "running" && item.status !== "waiting")
      return false;
    if (item.type === "subagent" && item.origin === "app_owned" && item.childThreadId !== null)
      return false;
    const work = cancelledTurnItemWork(item);
    return (
      work !== undefined &&
      isAwaitedWork(work) &&
      awaitingProviderThreadId(item, projection.runs) === providerThread.id
    );
  });
  return (
    openItem ||
    (providerThread.ownerNodeId === null &&
      (providerThread.pendingBackgroundTasks ?? []).some((task) =>
        isAwaitedWork(cancelledRosterTaskWork(task)),
      ))
  );
}

type RestartProjection = Pick<
  ProjectionRuntimeRecoveryState,
  | "thread"
  | "runs"
  | "providerThreads"
  | "providerSessions"
  | "providerTurns"
  | "runtimeRequests"
  | "turnItems"
>;

function latestRestartSource(projection: RestartProjection) {
  // Queued runs never started; recovery holds them behind the cut run.
  return projection.runs.reduce<OrchestrationV2Run | undefined>(
    (latest, candidate) =>
      candidate.status !== "queued" &&
      !neverStarted(candidate) &&
      (!latest || runRanAfter(candidate, latest))
        ? candidate
        : latest,
    undefined,
  );
}

function restartContinuationReason(
  projection: RestartProjection,
  run: OrchestrationV2Run,
  now: DateTime.Utc,
): RestartCutReason | undefined {
  if (projection.thread.archivedAt !== null || projection.thread.deletedAt !== null)
    return RESTART_CUT_REASONS.THREAD_INACTIVE;
  const preparedContinuation =
    run.status === "starting" && run.restartContinuationOfRunId !== undefined;
  const settled = run.status === "completed" || run.status === "waiting";
  if (run.status !== "running" && !preparedContinuation && !settled)
    return RESTART_CUT_REASONS.NOT_STARTED;
  if (restartContinuationBlocked(projection, run, now, settled))
    return RESTART_CUT_REASONS.NO_CONTINUATION;
  const liveTurnRequired = !preparedContinuation && !settled;
  if (projection.thread.providerInstanceId !== run.providerInstanceId)
    return RESTART_CUT_REASONS.PROVIDER_CHANGED;
  const providerThread = projection.providerThreads.find(
    (thread) => thread.id === run.providerThreadId,
  );
  if (
    !providerThread ||
    providerThread.appThreadId !== projection.thread.id ||
    providerThread.ownerNodeId !== null ||
    providerThread.providerInstanceId !== run.providerInstanceId ||
    providerThread.nativeThreadRef?.nativeId == null ||
    providerThread.nativeThreadRef.strength !== "strong" ||
    providerThread.nativeThreadRef.driver !== providerThread.driver ||
    providerThread.status === "closed" ||
    providerThread.status === "archived"
  )
    return RESTART_CUT_REASONS.NOT_RESUMABLE;
  const session = projection.providerSessions.find(
    (candidate) => candidate.id === providerThread.providerSessionId,
  );
  // Recovery stops shared sessions while visiting other bound threads. The
  // run's own live turn establishes liveness; a prior runtime error still rules it out.
  if (
    session === undefined ||
    session.providerInstanceId !== run.providerInstanceId ||
    session.driver !== providerThread.driver
  )
    return RESTART_CUT_REASONS.NOT_RESUMABLE;
  if (liveTurnRequired && session.status === "error") return RESTART_CUT_REASONS.SESSION_FAILED;
  // Settled turns have no live attempt to distinguish this cut from an older crash.
  if (settled) {
    if (session.status === "stopped" || session.status === "error")
      return RESTART_CUT_REASONS.NOT_RESUMABLE;
    return hasOpenAwaitedWork(projection, providerThread)
      ? undefined
      : RESTART_CUT_REASONS.BACKGROUND_ONLY;
  }
  if (
    liveTurnRequired &&
    !projection.providerTurns.some(
      (turn) =>
        turn.providerThreadId === providerThread.id &&
        turn.runAttemptId === run.activeAttemptId &&
        turn.status === "running",
    )
  )
    return RESTART_CUT_REASONS.NO_LIVE_TURN;
}

export function restartContinuationRun(
  projection: RestartProjection,
  now: DateTime.Utc,
): OrchestrationV2Run | undefined {
  const run = latestRestartSource(projection);
  return run !== undefined && restartContinuationReason(projection, run, now) === undefined
    ? run
    : undefined;
}

type RestartCutDecision =
  | { readonly run: OrchestrationV2Run; readonly cut: { readonly continuation: "scheduled" } }
  | {
      readonly run: OrchestrationV2Run;
      readonly cut: { readonly continuation: "declined"; readonly reason: RestartCutReason };
    };

/** Decide only a run this reconciliation actually cancels; checkpoint replay is not a cut. */
export function restartCutDecision(
  projection: RestartProjection,
  cancelled: ReadonlySet<RunId>,
  enabled: boolean | undefined,
  now: DateTime.Utc,
): RestartCutDecision | undefined {
  const run = latestRestartSource(projection);
  if (run === undefined || !cancelled.has(run.id)) return;
  const reason =
    projection.thread.archivedAt !== null || projection.thread.deletedAt !== null
      ? RESTART_CUT_REASONS.THREAD_INACTIVE
      : run.status !== "running" &&
          run.status !== "waiting" &&
          !(run.status === "starting" && run.restartContinuationOfRunId !== undefined)
        ? RESTART_CUT_REASONS.NOT_STARTED
        : enabled === undefined
          ? RESTART_CUT_REASONS.SETTINGS_UNAVAILABLE
          : !enabled
            ? RESTART_CUT_REASONS.CONTINUATION_OFF
            : restartContinuationReason(projection, run, now);
  return reason === undefined
    ? { run, cut: { continuation: "scheduled" } }
    : { run, cut: { continuation: "declined", reason } };
}

export const continueRestartedRun = Effect.fn("RestartContinuation.continueRestartedRun")(
  function* (input: { readonly threadId: ThreadId; readonly sourceRunId: RunId }) {
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const messageId = MessageId.make(`message:restart-continuation:${input.sourceRunId}`);
    const projection = yield* threads.getThreadRecords(
      input.threadId,
      ["messages", "runs", "providerTurns", "attempts", "runtimeRequests", "turnItems"],
      { messageIds: [messageId], turnItemTypes: ["secret_request"] },
    );
    if (projection.messages.some((message) => message.id === messageId)) return;
    const source = projection.runs.find((run) => run.id === input.sourceRunId);
    // A native note continuation may wake a settled run that lost awaited work.
    if (!source || source.restartCut?.continuation === "declined") return;
    const decline = (reason: RestartCutReason) =>
      threads.recordRestartCutDecline(input.threadId, input.sourceRunId, reason);
    if (!isRestartContinuationSource(source, projection.providerTurns)) {
      if (source.status === "cancelled") yield* decline(RESTART_CUT_REASONS.BACKGROUND_ONLY);
      return;
    }
    // Settings failures use the worker's existing retry/failure path, never a successful no-op.
    const settings = yield* ServerSettings.ServerSettingsService;
    const enabled = yield* settings.getSettings;
    if (
      !resolveProjectSettings(enabled, projection.thread.projectId).settings
        .continueThreadsAfterServerUpdate
    ) {
      yield* decline(RESTART_CUT_REASONS.CONTINUATION_OFF);
      return;
    }
    if (
      restartContinuationBlocked(
        projection,
        source,
        yield* DateTime.now,
        isRestartNoteSource(source, projection.providerTurns),
      )
    ) {
      yield* decline(RESTART_CUT_REASONS.NO_CONTINUATION);
      return;
    }
    // Bound consecutive crash recovery without creating another retry owner.
    let chainLength = 0;
    const loopWindowStart =
      DateTime.toEpochMillis(source.completedAt ?? source.requestedAt) -
      CONTINUATION_LOOP_WINDOW_MS;
    for (
      let run: OrchestrationV2Run | undefined = source;
      run?.restartContinuationOfRunId !== undefined && chainLength < MAX_CONTINUATION_CHAIN;
      run = projection.runs.find((candidate) => candidate.id === run?.restartContinuationOfRunId)
    ) {
      if (DateTime.toEpochMillis(run.requestedAt) < loopWindowStart) break;
      chainLength += 1;
    }
    if (chainLength >= MAX_CONTINUATION_CHAIN) {
      yield* decline(RESTART_CUT_REASONS.INVALID_CHAIN);
      return;
    }
    // Archive, provider changes and newer work are decided again at dispatch,
    // in the same transaction that records their refusal.
    const sourceRecords = yield* threads.getThreadRecords(
      input.threadId,
      ["messages", "turnItems"],
      {
        messageIds: [source.userMessageId],
        turnItemRunIds: [source.id],
        turnItemTypes: ["run_interrupt_request"],
      },
    );
    if (
      sourceRecords.turnItems.some(
        (item) => item.runId === source.id && item.type === "run_interrupt_request",
      )
    ) {
      yield* decline(RESTART_CUT_REASONS.STOP_REQUESTED);
      return;
    }
    const sourceMessage = sourceRecords.messages.find(
      (message) => message.id === source.userMessageId,
    );
    if (sourceMessage !== undefined && isNativeMaintenanceCommand(sourceMessage)) {
      yield* decline(RESTART_CUT_REASONS.MAINTENANCE);
      return;
    }
    const note = restartContinuationNote(
      source,
      projection.runs,
      projection.providerTurns,
      projection.attempts,
    );
    const noteText =
      note.work.length === 0 ? undefined : restartCancelledBackgroundWorkNote(note.work);
    const text =
      noteText === undefined
        ? CONTINUE_PROMPT
        : note.settled
          ? noteText
          : `${noteText}\n\n${CONTINUE_PROMPT}`;
    yield* threads
      .dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`command:restart-continuation:${input.sourceRunId}`),
        threadId: input.threadId,
        messageId,
        text,
        notification: {
          source: { kind: "system" },
          outcome: "updated",
          summary: note.settled
            ? "T3 Code restarted and stopped background work"
            : "T3 Code restarted and resumed this turn",
          detail: text,
        },
        attachments: [],
        modelSelection: source.modelSelection,
        dispatchMode: { type: "start_immediately" },
        createdBy: "agent",
        creationSource: "server",
        restartContinuationOfRunId: input.sourceRunId,
      })
      .pipe(
        Effect.catchTags({
          ThreadManagementProviderInstanceDisabledError: () =>
            decline(RESTART_CUT_REASONS.NOT_RESUMABLE),
        }),
      );
  },
  // Declined child continuations still owe parent results; existing continuation runs settle them instead.
  (effect, input) =>
    effect.pipe(
      Effect.andThen(
        Effect.gen(function* () {
          const threads = yield* ThreadManagementService.ThreadManagementService;
          yield* threads.recoverDelegatedTask(input.threadId, input.sourceRunId);
        }),
      ),
    ),
);
