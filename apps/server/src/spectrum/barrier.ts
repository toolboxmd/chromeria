import {
  CommandId,
  MessageId,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2Run,
  type RunId,
} from "@t3tools/contracts";

import type { SpectrumRound, SpectrumState } from "./state.ts";

/** Plans one round; dispatch and persistence belong to the serialized controller. */
export function nextRound(state: SpectrumState): SpectrumRound | null {
  if (state.status !== "active" || state.outbox.length > 0) return null;
  if (state.mode === "council" && state.limit < 2) {
    throw new RangeError("Council requires at least two relay rounds.");
  }
  if (state.moderator >= state.participants.length) {
    throw new RangeError("Moderator must name a participating Color.");
  }
  const previous = state.round;
  if (previous !== null && previous.generation !== state.generation) return null;
  if (previous !== null && !barrierComplete(previous)) return null;
  const step = previous === null ? 0 : previous.step + 1;
  if (
    (state.mode === "free" && step >= state.limit) ||
    (state.mode === "council" && step > state.limit + 1)
  )
    return null;
  const phase =
    state.mode === "free"
      ? "free"
      : step === 0
        ? "independent"
        : step === state.limit + 1
          ? "synthesis"
          : "relay";
  const participants =
    phase === "free"
      ? [state.participants[step % state.participants.length]!]
      : phase === "synthesis"
        ? [state.participants[state.moderator]!]
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
  if (["failed", "interrupted", "cancelled", "rolled_back"].includes(input.run.status)) {
    return { type: "failed", reason: "run-ended" };
  }
  if (input.run.status !== "completed") return { type: "waiting" };
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
