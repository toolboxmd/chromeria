import {
  EventId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type SpectrumTranscriptAppend,
  TurnItemId,
} from "@t3tools/contracts";
import type * as DateTime from "effect/DateTime";

import type { SpectrumMutation, SpectrumState } from "./state.ts";

/** Pure command planning. Revision/generation must be rechecked in the commit plan. */
export function planTranscriptAppend(
  state: SpectrumState,
  thread: OrchestrationV2AppThread,
  command: SpectrumTranscriptAppend,
  now: DateTime.Utc,
): {
  readonly mutation: SpectrumMutation;
  readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
} {
  if (
    command.threadId !== state.threadId ||
    thread.id !== state.threadId ||
    command.generation !== state.generation ||
    command.revision !== state.revision ||
    state.status === "retired" ||
    thread.deletedAt !== null ||
    thread.activeProviderThreadId !== null ||
    state.transcript.includes(command.messageId)
  )
    throw new RangeError("Spectrum transcript append does not match the registered controller.");
  const entryId = TurnItemId.make(`spectrum:transcript:${command.messageId}`);
  const base = {
    threadId: state.threadId,
    occurredAt: now,
  };
  return {
    mutation: {
      expectedRevision: state.revision,
      expectedGeneration: state.generation,
      state: {
        ...state,
        revision: state.revision + 1,
        transcript: [...state.transcript, command.messageId],
        inbox: command.role === "user" ? [...state.inbox, command.messageId] : state.inbox,
        outbox: state.outbox.filter((pending) => pending.commandId !== command.commandId),
      },
    },
    events: [
      {
        ...base,
        id: EventId.make(`${command.commandId}:message`),
        type: "message.updated",
        payload: {
          id: command.messageId,
          threadId: state.threadId,
          createdBy: command.role === "user" ? "user" : "system",
          creationSource: "server",
          runId: null,
          nodeId: null,
          role: command.role,
          text: command.text,
          attachments: [],
          streaming: false,
          createdAt: now,
          updatedAt: now,
        },
      },
      {
        ...base,
        id: EventId.make(`${command.commandId}:item`),
        type: "turn-item.updated",
        payload: {
          id: entryId,
          threadId: state.threadId,
          runId: null,
          nodeId: null,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: 0,
          status: "completed",
          title: null,
          startedAt: now,
          completedAt: now,
          updatedAt: now,
          ...(command.role === "user"
            ? ({
                type: "user_message",
                createdBy: "user",
                creationSource: "server",
                messageId: command.messageId,
                inputIntent: "turn_start",
                text: command.text,
                attachments: [],
              } as const)
            : ({
                type: "assistant_message",
                messageId: command.messageId,
                text: command.text,
                streaming: false,
              } as const)),
        },
      },
    ],
  };
}
