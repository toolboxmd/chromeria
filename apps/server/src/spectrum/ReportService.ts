import { CommandId, type OrchestrationV2Command, ThreadId } from "@t3tools/contracts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import type { ForkCommitPlan } from "../childThreads/ForkCommitPlan.ts";
import { ForkDispatchPlans } from "../fork/ForkDispatchPlans.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import {
  abandonedState,
  attemptOutcome,
  makeInitialReport,
  reportAdmission,
  reportAttempt,
  reportAttempts,
  reportCancelAdmission,
  reportCancellation,
  reportChainsUnchanged,
  reportStatePlan,
  reportUndeliveredGuard,
  retriedState,
  retryEvidence,
  type ReportCommand,
} from "./reportPolicy.ts";
import { spectrumPlan } from "./spectrumPlan.ts";
import type { SpectrumMutation, SpectrumState } from "./state.ts";
import { readSpectrum } from "./store.ts";

export class SpectrumReportError extends Schema.TaggedError<SpectrumReportError>()(
  "SpectrumReportError",
  { threadId: ThreadId, cause: Schema.Defect() },
) {}

const isReportError = Schema.is(SpectrumReportError);

/**
 * Delivers a settled Spectrum's report to its caller (D40, D42). The
 * controller calls settle once the participant barrier drained and deliver
 * whenever the report's runs change; abandon is the authenticated human route.
 */
export class SpectrumReportService extends Context.Service<
  SpectrumReportService,
  {
    readonly settle: (
      threadId: ThreadId,
      text: string,
      retired?: boolean,
    ) => Effect.Effect<SpectrumState, SpectrumReportError>;
    readonly deliver: (threadId: ThreadId) => Effect.Effect<SpectrumState, SpectrumReportError>;
    readonly abandon: (
      threadId: ThreadId,
      commandId: CommandId,
      person: string,
    ) => Effect.Effect<SpectrumState, SpectrumReportError>;
  }
>()("t3/spectrum/ReportService/SpectrumReportService") {}

const make = Effect.gen(function* () {
  const locks = yield* KeyedLock.make<ThreadId>();
  const sql = yield* SqlClient.SqlClient;
  const sink = yield* EventSink.EventSinkV2;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const withSql = <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient>) =>
    Effect.provideService(effect, SqlClient.SqlClient, sql);
  const fail = (threadId: ThreadId, cause: string) => new SpectrumReportError({ threadId, cause });
  const read = Effect.fnUntraced(function* (threadId: ThreadId) {
    const found = yield* withSql(readSpectrum(threadId));
    if (Option.isNone(found)) return yield* fail(threadId, "Spectrum is not registered");
    return found.value;
  });
  /** Settle uses the transcript's plan; report bookkeeping commits even after the transcript retired. */
  const persist = Effect.fnUntraced(function* (
    commandId: CommandId,
    commandType: string,
    old: SpectrumState,
    next: SpectrumState,
    plans: ReadonlyArray<ForkCommitPlan> = [],
  ) {
    const mutation: SpectrumMutation = {
      expectedRevision: old.revision,
      expectedGeneration: old.generation,
      state: next,
    };
    yield* sink.commitCommand({
      commandId,
      threadId: old.threadId,
      commandType,
      acceptedAt: yield* DateTime.now,
      events: [],
      effects: [],
      forkPlans: [
        commandType === "spectrum.report.settle"
          ? spectrumPlan(mutation)
          : reportStatePlan(mutation),
        ...plans,
      ],
    });
    return yield* read(old.threadId);
  });
  /** A rejected send is still a send: its durable receipt is the attempt's outcome. */
  const dispatch = (command: OrchestrationV2Command, plans: ReadonlyArray<ForkCommitPlan>) =>
    orchestrator.dispatch(command).pipe(
      Effect.provideService(ForkDispatchPlans, plans),
      Effect.asVoid,
      Effect.catch((cause) =>
        sql<{ readonly status: string }>`
          SELECT status FROM orchestration_command_receipts WHERE command_id = ${command.commandId}
        `.pipe(
          Effect.flatMap((rows) =>
            rows[0]?.status === "rejected" ? Effect.void : Effect.fail(cause),
          ),
        ),
      ),
    );
  // Replay sends the persisted command unchanged; admission is rebuilt from durable state each time.
  const send = Effect.fnUntraced(function* (state: SpectrumState, report: ReportCommand) {
    yield* dispatch(report, [reportAdmission(state.threadId, report)]);
    return yield* persist(
      CommandId.make(`${report.commandId}:sent`),
      "spectrum.report.sent",
      state,
      {
        ...state,
        revision: state.revision + 1,
        outbox: state.outbox.filter((command) => command.commandId !== report.commandId),
      },
    );
  });
  const pending = (state: SpectrumState) =>
    state.report !== null &&
    state.outbox.some((command) => command.commandId === state.report?.commandId);
  /** Cancels whatever still runs on the abandoned report's exact chains; later continuations are cancelled on later calls. */
  const drain = Effect.fnUntraced(function* (state: SpectrumState) {
    const report = state.report!;
    for (const attempt of reportAttempts(state)) {
      const outcome = yield* withSql(attemptOutcome(state.callerThreadId, attempt));
      if (outcome.kind !== "open") continue;
      yield* dispatch(reportCancellation(attempt, outcome.tail), [
        reportCancelAdmission(state.threadId, report, {
          messageId: attempt.messageId,
          runIds: outcome.runIds,
        }),
      ]);
    }
    return yield* read(state.threadId);
  });

  const settle = Effect.fn("SpectrumReport.settle")(function* (
    threadId: ThreadId,
    text: string,
    retired: boolean,
  ) {
    const state = yield* read(threadId);
    if (state.report !== null) {
      const attempt = reportAttempt(state);
      if (attempt?.generation === state.generation && attempt.cycle === state.cycle) return state;
      // An earlier cycle's report is replaced only once delivered; reopen clears an abandoned one.
      const outcome = yield* withSql(attemptOutcome(state.callerThreadId, state.report));
      if (outcome.kind !== "delivered")
        return yield* fail(threadId, "An earlier Spectrum report is still undelivered");
    }
    const next = { ...makeInitialReport(state, text, retired), revision: state.revision + 1 };
    return yield* persist(
      CommandId.make(`${next.report!.commandId}:settle`),
      "spectrum.report.settle",
      state,
      next,
    );
  });

  const deliver = Effect.fn("SpectrumReport.deliver")(function* (threadId: ThreadId) {
    let state = yield* read(threadId);
    const report = state.report;
    if (report === null) return state;
    if (state.reportAbandonment?.commandId === report.commandId) return yield* drain(state);
    // A report this module did not write holds for a person instead of restarting its budget.
    if (reportAttempt(state) === null) return state;
    if (pending(state)) state = yield* send(state, report);
    const chains = yield* withSql(retryEvidence(state));
    if (chains === null) return state;
    const next = retriedState(state);
    state = yield* persist(
      CommandId.make(`${next.report!.commandId}:retry`),
      "spectrum.report.retry",
      state,
      next,
      [reportChainsUnchanged(threadId, chains)],
    );
    return pending(state) ? yield* send(state, state.report!) : state;
  });

  const abandon = Effect.fn("SpectrumReport.abandon")(function* (
    threadId: ThreadId,
    commandId: CommandId,
    person: string,
  ) {
    const state = yield* read(threadId);
    const report = state.report;
    if (report?.commandId !== commandId)
      return yield* fail(threadId, "Only the current Spectrum report can be abandoned");
    if (person.trim().length === 0)
      return yield* fail(threadId, "A person must abandon the Spectrum report");
    if (state.reportAbandonment?.commandId === commandId) return yield* drain(state);
    const outcome = yield* withSql(attemptOutcome(state.callerThreadId, report));
    if (outcome.kind === "delivered")
      return yield* fail(threadId, "The Spectrum report was already delivered");
    // The turn can complete between this read and the commit; the guard rechecks it there.
    const abandoned = yield* persist(
      CommandId.make(`${commandId}:abandon`),
      "spectrum.report.abandon",
      state,
      abandonedState(state, person, yield* DateTime.now),
      [reportUndeliveredGuard(threadId, report)],
    );
    return yield* drain(abandoned);
  });

  const serialized = <A, E>(threadId: ThreadId, effect: Effect.Effect<A, E>) =>
    locks
      .withLock(threadId, effect)
      .pipe(
        Effect.mapError((cause) =>
          isReportError(cause) ? cause : new SpectrumReportError({ threadId, cause }),
        ),
      );
  return SpectrumReportService.of({
    settle: (threadId, text, retired = false) =>
      serialized(threadId, settle(threadId, text, retired)),
    deliver: (threadId) => serialized(threadId, deliver(threadId)),
    abandon: (threadId, commandId, person) =>
      serialized(threadId, abandon(threadId, commandId, person)),
  });
});

export const layer = Layer.effect(SpectrumReportService, make);
