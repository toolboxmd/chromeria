import {
  CommandId,
  MessageId,
  ModelSelection,
  NodeId,
  NonNegativeInt,
  OrchestrationV2Command,
  RunId,
  ScheduledTaskId,
  SpectrumTranscriptAppend,
  ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";

export const SpectrumReport = OrchestrationV2Command.pipe(
  Schema.refine(
    (command): command is Extract<OrchestrationV2Command, { type: "message.dispatch" }> =>
      command.type === "message.dispatch",
  ),
);

export const Participant = Schema.Struct({
  threadId: ThreadId,
  label: Schema.String,
  selection: ModelSelection,
  /** Later candidates, tried in order only while this slot's Color run is not committed. */
  fallbacks: Schema.Array(ModelSelection).pipe(Schema.withDecodingDefault(Effect.succeed([]))),
  /** Candidates that failed admission, so a restart never repeats them. */
  attempted: Schema.Array(ModelSelection).pipe(Schema.withDecodingDefault(Effect.succeed([]))),
  instructions: Schema.optional(Schema.String),
});
export type Participant = typeof Participant.Type;

export const SpectrumSlot = Schema.Struct({
  threadId: ThreadId,
  commandId: CommandId,
  messageId: MessageId,
  runId: Schema.NullOr(RunId),
  replyIds: Schema.NullOr(Schema.Array(MessageId)),
});
export type SpectrumSlot = typeof SpectrumSlot.Type;

export const SpectrumRound = Schema.Struct({
  generation: NonNegativeInt,
  step: NonNegativeInt,
  phase: Schema.Literals(["independent", "relay", "synthesis", "free", "moderation", "summary"]),
  slots: Schema.Array(SpectrumSlot),
});
export type SpectrumRound = typeof SpectrumRound.Type;

/** Free mode's persisted moderation progress, so a restart never repeats a White call. */
export const FreeProgress = Schema.Struct({
  cycle: NonNegativeInt,
  /** Color turns taken this cycle, the opening round included. */
  turns: NonNegativeInt,
  next: Schema.Union([
    Schema.Struct({ type: Schema.Literal("color"), participant: NonNegativeInt }),
    Schema.Struct({
      type: Schema.Literal("moderation"),
      reask: Schema.Boolean,
      retried: Schema.Boolean,
    }),
    Schema.Struct({ type: Schema.Literal("summary"), retried: Schema.Boolean }),
  ]),
});
export type FreeProgress = typeof FreeProgress.Type;

// Only durable command data goes in the outbox, never closures or provider handles.
const OutboxCommand = Schema.Union([OrchestrationV2Command, SpectrumTranscriptAppend]);
export const SpectrumState = Schema.Struct({
  version: Schema.Literal(2),
  threadId: ThreadId,
  callerThreadId: ThreadId,
  callerRunId: Schema.NullOr(RunId),
  scheduledTaskId: Schema.NullOr(ScheduledTaskId),
  schedulerRunId: Schema.NullOr(Schema.String),
  question: Schema.String,
  mode: Schema.Literals(["council", "free"]),
  turnsPerColor: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 12 })).pipe(
    Schema.withDecodingDefault(Effect.succeed(3)),
  ),
  limit: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
  /** Spectra started before #525 have no separate White: the Color at this index acts as White. */
  moderator: Schema.optionalKey(NonNegativeInt),
  /** White's own member: it writes the synthesis, moderation and summary, never a Color turn. */
  white: Schema.optionalKey(Participant),
  participants: Schema.Array(Participant).check(
    Schema.isMinLength(2),
    Schema.isMaxLength(8),
    Schema.makeFilter((participants) =>
      new Set(participants.map((participant) => participant.threadId)).size === participants.length
        ? true
        : "Spectrum participants must have distinct thread IDs.",
    ),
  ),
  generation: NonNegativeInt,
  revision: NonNegativeInt,
  status: Schema.Literals(["active", "settled", "retired"]),
  cycle: NonNegativeInt,
  round: Schema.NullOr(SpectrumRound),
  transcript: Schema.Array(MessageId),
  inbox: Schema.Array(MessageId),
  outbox: Schema.Array(OutboxCommand),
  free: Schema.NullOr(FreeProgress).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
  report: Schema.NullOr(SpectrumReport).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
  /** The caller's delegated task row for an agent-started Spectrum; user starts have none. */
  delegatedTaskId: Schema.NullOr(NodeId).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
  reportAbandonment: Schema.NullOr(
    Schema.Struct({
      commandId: CommandId,
      person: Schema.String,
      abandonedAt: Schema.DateTimeUtcFromString,
    }),
  ).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
}).check(
  Schema.makeFilter((state) => {
    if (state.white === undefined)
      return state.moderator !== undefined && state.moderator < state.participants.length
        ? true
        : "White must be its own member or a participating Color.";
    if (state.moderator !== undefined) return "White cannot be both a member and a Color.";
    return state.participants.every((color) => color.threadId !== state.white!.threadId)
      ? true
      : "White must have its own thread.";
  }),
);
export type SpectrumState = typeof SpectrumState.Type;

export const whiteOf = (state: SpectrumState): Participant =>
  state.white ?? state.participants[state.moderator!]!;

/** Every member with a thread: the Colors, then a separate White. */
export const membersOf = (state: SpectrumState): ReadonlyArray<Participant> =>
  state.white === undefined ? state.participants : [...state.participants, state.white];

export const memberOf = (state: SpectrumState, threadId: string): Participant | undefined =>
  membersOf(state).find((member) => member.threadId === threadId);

/** Consumed by the shared fork commit seam, inside the command transaction. */
export const SpectrumMutation = Schema.Struct({
  expectedRevision: NonNegativeInt,
  expectedGeneration: NonNegativeInt,
  state: SpectrumState,
});
export type SpectrumMutation = typeof SpectrumMutation.Type;
