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
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import { bindRun, nextRound } from "./barrier.ts";
import { spectrumLaunchAdmission } from "./launchAdmission.ts";
import { spectrumPlan } from "./spectrumPlan.ts";
import type { SpectrumState } from "./state.ts";
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
  const locks = yield* KeyedLock.make<ThreadId>();
  const sql = yield* SqlClient.SqlClient;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sink = yield* EventSink.EventSinkV2;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const read = (threadId: ThreadId) =>
    readSpectrum(threadId).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.flatMap((state) =>
        Option.isSome(state)
          ? Effect.succeed(state.value)
          : Effect.fail(new SpectrumRoundError({ threadId, cause: "Spectrum is not registered" })),
      ),
    );
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
      const participant = state.participants.find((color) => color.threadId === slot.threadId)!;
      const instruction =
        round.phase === "synthesis"
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
    commandId: CommandId,
  ) {
    const state = yield* read(threadId);
    const pending = state.outbox.find((command) => command.commandId === commandId);
    if (pending?.type !== "message.dispatch" || state.round === null)
      return yield* new SpectrumRoundError({
        threadId,
        cause: "Round command is absent from durable state",
      });
    // Fiber-local admission is rebuilt on every replay. No captured scope survives a restart.
    const result = yield* orchestrator
      .dispatch(pending)
      .pipe(
        Effect.provideService(ForkDispatchPlans, [
          spectrumLaunchAdmission(threadId, state.generation, pending),
        ]),
      );
    const run = result.storedEvents.find(
      (stored) =>
        stored.event.type === "run.created" &&
        stored.event.payload.userMessageId === pending.messageId,
    )?.event;
    if (run?.type !== "run.created")
      return yield* new SpectrumRoundError({
        threadId,
        cause: "Accepted Color launch has no exact run binding",
      });
    const round = bindRun(state.round, {
      generation: state.generation,
      commandId,
      run: run.payload,
    });
    return yield* persist(CommandId.make(`${commandId}:bound`), state, {
      ...state,
      revision: state.revision + 1,
      round,
      outbox: state.outbox.filter((command) => command.commandId !== commandId),
    });
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
