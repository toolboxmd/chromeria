import {
  CommandId,
  MessageId,
  ModelSelection,
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

const Participant = Schema.Struct({
  threadId: ThreadId,
  label: Schema.String,
  selection: ModelSelection,
  instructions: Schema.optional(Schema.String),
});

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
  phase: Schema.Literals(["independent", "relay", "synthesis", "free"]),
  slots: Schema.Array(SpectrumSlot),
});
export type SpectrumRound = typeof SpectrumRound.Type;

// Only durable command data goes in the outbox, never closures or provider handles.
const OutboxCommand = Schema.Union([OrchestrationV2Command, SpectrumTranscriptAppend]);
export const SpectrumState = Schema.Struct({
  version: Schema.Literal(1),
  threadId: ThreadId,
  callerThreadId: ThreadId,
  callerRunId: Schema.NullOr(RunId),
  scheduledTaskId: Schema.NullOr(ScheduledTaskId),
  schedulerRunId: Schema.NullOr(Schema.String),
  question: Schema.String,
  mode: Schema.Literals(["council", "free"]),
  limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 })),
  moderator: NonNegativeInt,
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
  cursor: NonNegativeInt,
  status: Schema.Literals(["active", "settled", "retired"]),
  cycle: NonNegativeInt,
  round: Schema.NullOr(SpectrumRound),
  transcript: Schema.Array(MessageId),
  inbox: Schema.Array(MessageId),
  outbox: Schema.Array(OutboxCommand),
  report: Schema.NullOr(SpectrumReport).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
  reportAbandonment: Schema.NullOr(
    Schema.Struct({
      commandId: CommandId,
      person: Schema.String,
      abandonedAt: Schema.DateTimeUtcFromString,
    }),
  ).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
});
export type SpectrumState = typeof SpectrumState.Type;

/** Consumed by the shared fork commit seam, inside the command transaction. */
export const SpectrumMutation = Schema.Struct({
  expectedRevision: NonNegativeInt,
  expectedGeneration: NonNegativeInt,
  state: SpectrumState,
});
export type SpectrumMutation = typeof SpectrumMutation.Type;
