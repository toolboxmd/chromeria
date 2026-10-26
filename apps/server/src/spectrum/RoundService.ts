import * as PeerAuthority from "../peer/PeerAuthority.ts";
import { PeerDispatchAuthority } from "../peer/peerEffectAuthority.ts";
import { CommandId, type OrchestrationV2Command, ThreadId } from "@t3tools/contracts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { ForkDispatchPlans } from "../fork/ForkDispatchPlans.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapters from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as Prism from "../prism/PrismService.ts";
import { bindRun, fallBack, nextRound } from "./barrier.ts";
import { whiteInstruction } from "./freeMode.ts";
import { spectrumLaunchAdmission } from "./launchAdmission.ts";
import { spectrumPlan } from "./spectrumPlan.ts";
import { memberOf, type SpectrumState } from "./state.ts";
import { readSpectrum } from "./store.ts";

export class SpectrumRoundError extends Schema.TaggedError<SpectrumRoundError>()(
  "SpectrumRoundError",
  { threadId: ThreadId, cause: Schema.Defect() },
) {}

export class SpectrumRoundService extends Context.Service<
  SpectrumRoundService,
  {
    readonly prepare: (threadId: ThreadId) => Effect.Effect<SpectrumState, SpectrumRoundError>;
    readonly dispatch: (
      threadId: ThreadId,
      commandId: CommandId,
    ) => Effect.Effect<SpectrumState, SpectrumRoundError>;
  }
>()("t3/spectrum/RoundService/SpectrumRoundService") {}

const make = Effect.gen(function* () {
  const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
  const locks = yield* KeyedLock.make<ThreadId>();
  const sql = yield* SqlClient.SqlClient;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sink = yield* EventSink.EventSinkV2;
  const threads = yield* Threads.ThreadManagementService;
  const adapters = yield* ProviderAdapters.ProviderAdapterRegistryV2;
  const providers = yield* ProviderRegistry.ProviderRegistry;
  const read = (threadId: ThreadId) =>
    readSpectrum(threadId).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.flatMap((state) =>
        Option.isSome(state)
          ? Effect.succeed(state.value)
          : Effect.fail(new SpectrumRoundError({ threadId, cause: "Spectrum is not registered" })),
      ),
    );
  /** Admission refused the start: nothing of it committed, and its command can never commit. */
  const refusedBeforeCommit = (
    commandId: CommandId,
    error: { readonly _tag: string; readonly code?: string },
  ) =>
    error._tag === "ThreadManagementProviderInstanceDisabledError" ||
    (error._tag === "OrchestratorMcpFailure" &&
      (error.code === "provider_unavailable" || error.code === "model_unavailable"))
      ? Effect.succeed(true)
      : sql<{ readonly status: string }>`
          SELECT status FROM orchestration_command_receipts WHERE command_id = ${commandId}
        `.pipe(Effect.map((rows) => rows[0]?.status === "rejected"));
  const persist = Effect.fn("SpectrumRound.persist")(function* (
    commandId: CommandId,
    old: SpectrumState,
    next: SpectrumState,
  ) {
    yield* sink.commitCommand({
      commandId,
      threadId: old.threadId,
      commandType: "spectrum.round.advance",
      acceptedAt: yield* DateTime.now,
      events: [],
      effects: [],
      forkPlans: [
        spectrumPlan({
          expectedRevision: old.revision,
          expectedGeneration: old.generation,
          state: next,
        }),
      ],
    });
    return yield* read(old.threadId);
  });
  const prepare = Effect.fn("SpectrumRound.prepare")(function* (threadId: ThreadId) {
    const state = yield* read(threadId);
    const round = yield* Effect.try(() => nextRound(state));
    if (round === null) return state;
    const records = yield* projections.getThreadRecords(threadId, ["messages"]);
    const discussion = records.messages
      .filter(
        (message) =>
          state.transcript.includes(message.id) &&
          (round.phase !== "independent" || message.role === "user"),
      )
      .map((message) => message.text)
      .join("\n\n");
    const commands: ReadonlyArray<OrchestrationV2Command> = round.slots.map((slot) => {
      const participant = memberOf(state, slot.threadId)!;
      const instruction =
        round.phase === "moderation" || round.phase === "summary"
          ? whiteInstruction(state, round.phase)
          : round.phase === "synthesis"
            ? "Synthesize the council's final answer."
            : round.phase === "independent"
              ? "Answer independently before reading other Colors."
              : "Respond to the discussion and improve the answer.";
      return {
        type: "message.dispatch",
        commandId: slot.commandId,
        messageId: slot.messageId,
        threadId: slot.threadId,
        senderThreadId: threadId,
        createdBy: "system",
        creationSource: "server",
        attachments: [],
        modelSelection: participant.selection,
        dispatchMode: { type: "start_immediately" },
        text: [
          participant.instructions ?? "",
          instruction,
          `Question:\n${state.question}`,
          discussion.length === 0 ? "" : `Discussion:\n${discussion}`,
        ]
          .filter(Boolean)
          .join("\n\n"),
      };
    });
    return yield* persist(
      CommandId.make(
        `spectrum:${threadId}:${state.generation}:${state.cycle}:${round.step}:prepare`,
      ),
      state,
      { ...state, revision: state.revision + 1, round, inbox: [], outbox: commands },
    );
  });
  const dispatch = Effect.fn("SpectrumRound.dispatch")(function* (
    threadId: ThreadId,
    requested: CommandId,
  ) {
    let state = yield* read(threadId);
    let commandId = requested;
    while (true) {
      const current = state;
      const pending = current.outbox.find((command) => command.commandId === commandId);
      if (pending?.type !== "message.dispatch" || current.round === null)
        return yield* new SpectrumRoundError({
          threadId,
          cause: "Round command is absent from durable state",
        });
      const committed =
        (yield* sql`SELECT 1 FROM orchestration_command_receipts WHERE command_id=${pending.commandId} AND aggregate_kind='thread' AND aggregate_id=${pending.threadId} AND command_type='message.dispatch' AND status='accepted'`.pipe(
          Effect.mapError((cause) => new SpectrumRoundError({ threadId, cause })),
        )).length === 1;
      if (!committed && current.automationCertificateId !== undefined && Option.isNone(authority))
        return yield* new SpectrumRoundError({
          threadId,
          cause: "Native automation execution is unavailable",
        });
      const owner =
        committed || current.automationCertificateId === undefined || Option.isNone(authority)
          ? undefined
          : yield* authority.value.automationExecution(
              current.automationCertificateId,
              pending.commandId,
            );
      // Fiber-local admission is rebuilt on every replay. No captured scope survives a restart.
      const outcome = yield* Effect.gen(function* () {
        // Replays bind an already accepted run; only a new start needs fresh validation.
        if (!committed) {
          if (pending.modelSelection === undefined)
            return yield* new SpectrumRoundError({
              threadId,
              cause: "Round command has no model selection",
            });
          yield* Prism.validateLaunchSelection(pending.modelSelection).pipe(
            Effect.provideService(ProviderAdapters.ProviderAdapterRegistryV2, adapters),
            Effect.provideService(ProviderRegistry.ProviderRegistry, providers),
          );
        }
        return yield* threads.dispatch(pending);
      }).pipe(
        Effect.provideService(PeerDispatchAuthority, owner?.id),
        Effect.provideService(ForkDispatchPlans, [
          spectrumLaunchAdmission(threadId, current.generation, pending),
        ]),
        Effect.map((result) => ({ type: "dispatched" as const, result })),
        Effect.catch((error) =>
          Effect.gen(function* () {
            const fallback = fallBack(current, pending.commandId);
            if (fallback === null || !(yield* refusedBeforeCommit(pending.commandId, error)))
              return yield* error;
            return { type: "fallback" as const, fallback };
          }),
        ),
      );
      if (outcome.type === "fallback") {
        // The next candidate is durable before its start, so a restart never retries a refused one.
        state = yield* persist(
          CommandId.make(`${pending.commandId}:refused`),
          current,
          outcome.fallback,
        );
        commandId = outcome.fallback.round!.slots.find(
          (slot) => slot.threadId === pending.threadId && slot.runId === null,
        )!.commandId;
        continue;
      }
      const run = outcome.result.storedEvents.find(
        (stored) =>
          stored.event.type === "run.created" &&
          stored.event.payload.userMessageId === pending.messageId,
      )?.event;
      if (run?.type !== "run.created")
        return yield* new SpectrumRoundError({
          threadId,
          cause: "Accepted Color launch has no exact run binding",
        });
      const round = bindRun(current.round, {
        generation: current.generation,
        commandId,
        run: run.payload,
      });
      return yield* persist(CommandId.make(`${commandId}:bound`), current, {
        ...current,
        revision: current.revision + 1,
        round,
        outbox: current.outbox.filter((command) => command.commandId !== commandId),
      });
    }
  });
  return SpectrumRoundService.of({
    prepare: (threadId) =>
      locks
        .withLock(threadId, prepare(threadId))
        .pipe(Effect.mapError((cause) => new SpectrumRoundError({ threadId, cause }))),
    dispatch: (threadId, commandId) =>
      locks
        .withLock(threadId, dispatch(threadId, commandId))
        .pipe(Effect.mapError((cause) => new SpectrumRoundError({ threadId, cause }))),
  });
});

export const layer = Layer.effect(SpectrumRoundService, make);
