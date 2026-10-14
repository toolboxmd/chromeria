import { runRanAfter } from "@t3tools/shared/orchestrationV2ThreadError";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  CommandId,
  MessageId,
  type OrchestrationV2Run,
  type RunId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type { ProjectionRuntimeRecoveryState } from "./ProjectionStore.ts";
import { RESTART_CUT_REASONS, type RestartCutReason } from "../restartContinuation/RestartCut.ts";

import * as ServerSettings from "../serverSettings.ts";
import { isNativeMaintenanceCommand } from "./Orchestrator.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import {
  isRestartNoteSource,
  restartCancelledBackgroundWorkNote,
  restartContinuationNote,
} from "./RestartBackgroundNote.ts";

const CONTINUE_PROMPT = "Continue where you left off.";

/**
 * Resume only an unfinished root turn. Leftover background work is cleaned up
 * separately and reported on the next user turn; it must not wake a settled run.
 */
type RestartProjection = Pick<
  ProjectionRuntimeRecoveryState,
  "thread" | "runs" | "providerThreads" | "providerSessions" | "providerTurns"
>;

function latestRestartSource(projection: RestartProjection) {
  // Queued runs never started; recovery holds them behind the cut run.
  return projection.runs.reduce<OrchestrationV2Run | undefined>(
    (latest, candidate) =>
      candidate.status !== "queued" && (!latest || runRanAfter(candidate, latest))
        ? candidate
        : latest,
    undefined,
  );
}

function restartContinuationReason(
  projection: RestartProjection,
  run: OrchestrationV2Run,
): RestartCutReason | undefined {
  if (projection.thread.archivedAt !== null || projection.thread.deletedAt !== null)
    return RESTART_CUT_REASONS.THREAD_INACTIVE;
  const preparedContinuation =
    run.status === "starting" && run.restartContinuationOfRunId !== undefined;
  if (run.status !== "running" && !preparedContinuation) return RESTART_CUT_REASONS.NOT_STARTED;
  const liveTurnRequired = !preparedContinuation;
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
): OrchestrationV2Run | undefined {
  const run = latestRestartSource(projection);
  return run !== undefined && restartContinuationReason(projection, run) === undefined
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
): RestartCutDecision | undefined {
  const run = latestRestartSource(projection);
  if (run === undefined || !cancelled.has(run.id)) return;
  const reason =
    projection.thread.archivedAt !== null || projection.thread.deletedAt !== null
      ? RESTART_CUT_REASONS.THREAD_INACTIVE
      : run.status === "waiting"
        ? RESTART_CUT_REASONS.BACKGROUND_ONLY
        : run.status !== "running" &&
            !(run.status === "starting" && run.restartContinuationOfRunId !== undefined)
          ? RESTART_CUT_REASONS.NOT_STARTED
          : enabled === undefined
            ? RESTART_CUT_REASONS.SETTINGS_UNAVAILABLE
            : !enabled
              ? RESTART_CUT_REASONS.CONTINUATION_OFF
              : restartContinuationReason(projection, run);
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
      ["messages", "runs", "providerTurns", "attempts"],
      { messageIds: [messageId] },
    );
    if (projection.messages.some((message) => message.id === messageId)) return;
    const source = projection.runs.find((run) => run.id === input.sourceRunId);
    // Pending effects from older versions may target settled background work,
    // including waiting runs that reconciliation subsequently cancelled.
    if (!source || source.status !== "cancelled" || source.restartCut?.continuation === "declined")
      return;
    const decline = (reason: RestartCutReason) =>
      threads.recordRestartCutDecline(input.threadId, input.sourceRunId, reason);
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
    if (isRestartNoteSource(source, projection.providerTurns)) {
      yield* decline(RESTART_CUT_REASONS.BACKGROUND_ONLY);
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
    // The user asked this run to stop before the restart cut it.
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
    yield* threads.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`command:restart-continuation:${input.sourceRunId}`),
      threadId: input.threadId,
      messageId,
      text:
        noteText === undefined
          ? CONTINUE_PROMPT
          : note.settled
            ? noteText
            : `${noteText}\n\n${CONTINUE_PROMPT}`,
      attachments: [],
      modelSelection: source.modelSelection,
      dispatchMode: { type: "start_immediately" },
      createdBy: "agent",
      creationSource: "server",
      restartContinuationOfRunId: input.sourceRunId,
    });
  },
  // A delegated child this declined to continue still owes its parent a
  // result. Once a continuation run exists this is a no-op; that run settles it.
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
