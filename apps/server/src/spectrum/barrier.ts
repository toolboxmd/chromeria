import {
  CommandId,
  MessageId,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2Run,
  type RunId,
} from "@t3tools/contracts";

import { memberOf, whiteOf, type SpectrumRound, type SpectrumState } from "./state.ts";
import { runOutcome } from "../orchestration-v2/RestartBackgroundNote.ts";
import { freeSpeaker } from "./freeMode.ts";

/** Plans one round; dispatch and persistence belong to the serialized controller. */
export function nextRound(state: SpectrumState): SpectrumRound | null {
  if (state.status !== "active" || state.outbox.length > 0) return null;
  if (state.mode === "council" && state.limit < 2) {
    throw new RangeError("Council requires at least two relay rounds.");
  }
  const previous = state.round;
  if (previous !== null && previous.generation !== state.generation) return null;
  if (previous !== null && !barrierComplete(previous)) return null;
  const step = previous === null ? 0 : previous.step + 1;
  const speaker = state.mode === "free" ? freeSpeaker(state) : null;
  if (
    (state.mode === "free" && speaker === null) ||
    (state.mode === "council" && step > state.limit + 1)
  )
    return null;
  const phase =
    speaker !== null
      ? speaker.phase
      : step === 0
        ? "independent"
        : step === state.limit + 1
          ? "synthesis"
          : "relay";
  const participants =
    speaker !== null
      ? [speaker.member]
      : phase === "synthesis"
        ? [whiteOf(state)]
        : state.participants;
  return {
    generation: state.generation,
    step,
    phase,
    slots: participants.map((participant) => {
      const key = `spectrum:${state.threadId}:${state.generation}:${state.cycle}:${step}:${participant.threadId}`;
      return {
        threadId: participant.threadId,
        commandId: CommandId.make(`${key}:start`),
        messageId: MessageId.make(`${key}:input`),
        runId: null,
        replyIds: null,
      };
    }),
  };
}

export function barrierComplete(round: SpectrumRound): boolean {
  return round.slots.length > 0 && round.slots.every((slot) => slot.replyIds !== null);
}

/**
 * Replace a Color start its candidate could not get admitted for with the slot's next
 * candidate, under fresh identities: the refused command keeps its refusal receipt.
 */
export function fallBack(state: SpectrumState, commandId: CommandId): SpectrumState | null {
  const pending = state.outbox.find((command) => command.commandId === commandId);
  if (pending?.type !== "message.dispatch" || state.round === null) return null;
  const participant = memberOf(state, pending.threadId);
  const [next, ...rest] = participant?.fallbacks ?? [];
  if (participant === undefined || next === undefined) return null;
  const replacement = {
    ...participant,
    selection: next,
    fallbacks: rest,
    attempted: [...participant.attempted, participant.selection],
  };
  const retry = {
    commandId: CommandId.make(`${pending.commandId}:fallback`),
    messageId: MessageId.make(`${pending.messageId}:fallback`),
  };
  return {
    ...state,
    revision: state.revision + 1,
    participants: state.participants.map((color) => (color === participant ? replacement : color)),
    ...(state.white === participant ? { white: replacement } : {}),
    round: {
      ...state.round,
      slots: state.round.slots.map((slot) =>
        slot.commandId === commandId && slot.runId === null ? { ...slot, ...retry } : slot,
      ),
    },
    outbox: state.outbox.map((command) =>
      command.commandId === commandId ? { ...pending, ...retry, modelSelection: next } : command,
    ),
  };
}

/** Bind a persisted request, not whichever run happens to be latest in the child. */
export function bindRun(
  round: SpectrumRound,
  input: {
    readonly generation: number;
    readonly commandId: CommandId;
    readonly run: OrchestrationV2Run;
  },
): SpectrumRound {
  if (input.generation !== round.generation) return round;
  const index = round.slots.findIndex(
    (slot) =>
      slot.commandId === input.commandId &&
      slot.threadId === input.run.threadId &&
      slot.messageId === input.run.userMessageId &&
      slot.runId === null,
  );
  if (index === -1) return round;
  return {
    ...round,
    slots: round.slots.map((slot, i) => (i === index ? { ...slot, runId: input.run.id } : slot)),
  };
}

/** Prism's persisted retry relation is the only authority to replace a binding. */
export function continueRun(
  round: SpectrumRound,
  input: {
    readonly generation: number;
    readonly sourceRunId: RunId;
    readonly run: OrchestrationV2Run;
  },
): SpectrumRound {
  if (input.generation !== round.generation) return round;
  const index = round.slots.findIndex(
    (slot) =>
      slot.threadId === input.run.threadId &&
      slot.runId === input.sourceRunId &&
      slot.replyIds === null,
  );
  if (index === -1) return round;
  return {
    ...round,
    slots: round.slots.map((slot, i) => (i === index ? { ...slot, runId: input.run.id } : slot)),
  };
}

/** Caller supplies full run-scoped records, with source order retained by the read. */
export function acceptReply(
  round: SpectrumRound,
  input: {
    readonly generation: number;
    readonly run: OrchestrationV2Run;
    readonly messages: ReadonlyArray<OrchestrationV2ConversationMessage>;
    readonly recoveryPending: boolean;
  },
):
  | { readonly type: "ignored" | "waiting" }
  | { readonly type: "failed"; readonly reason: "run-ended" | "missing-reply" }
  | {
      readonly type: "accepted";
      readonly round: SpectrumRound;
      readonly messages: ReadonlyArray<OrchestrationV2ConversationMessage>;
    } {
  if (input.generation !== round.generation) return { type: "ignored" };
  const index = round.slots.findIndex(
    (slot) =>
      slot.threadId === input.run.threadId && slot.runId === input.run.id && slot.replyIds === null,
  );
  if (index === -1) return { type: "ignored" };
  if (input.recoveryPending) return { type: "waiting" };
  const outcome = runOutcome(input.run);
  if (["failed", "interrupted", "cancelled", "rolled_back"].includes(outcome)) {
    return { type: "failed", reason: "run-ended" };
  }
  if (outcome !== "completed") return { type: "waiting" };
  const messages = input.messages.filter(
    (message) =>
      message.threadId === input.run.threadId &&
      message.runId === input.run.id &&
      message.role === "assistant",
  );
  if (messages.some((message) => message.streaming)) return { type: "waiting" };
  if (messages.length === 0) return { type: "failed", reason: "missing-reply" };
  return {
    type: "accepted",
    round: {
      ...round,
      slots: round.slots.map((slot, i) =>
        i === index ? { ...slot, replyIds: messages.map((message) => message.id) } : slot,
      ),
    },
    messages,
  };
}
